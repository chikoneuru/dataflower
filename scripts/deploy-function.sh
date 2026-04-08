#!/bin/bash

# Script to deploy a serverless function to a specific node
set -e

if [ $# -lt 4 ]; then
    echo "Usage: $0 <function_name> <function_type> <function_category> <node_type> [function_step] [image_name]"
    echo "Example: $0 recognizer__adult recognizer ml compute-node-1 inference recognizer__adult"
    echo ""
    echo "Available node types:"
    echo "  - node-1 (node1)"
    echo "  - node-2 (node2)" 
    echo "  - node-3 (node3)"
    echo ""
    echo "Function categories:"
    echo "  - ml (machine learning)"
    echo "  - data (data processing)"
    echo "  - video (video processing)"
    echo "  - text (text processing)"
    echo "  - compute (general computation)"
    echo ""
    echo "Function steps (optional):"
    echo "  - start, process, merge, upload, etc."
    exit 1
fi

FUNCTION_NAME=$1
FUNCTION_TYPE=$2
FUNCTION_CATEGORY=$3
NODE_TYPE=$4
FUNCTION_STEP=${5:-"process"}
IMAGE_NAME=${6:-$1}

echo "🚀 Deploying function: $FUNCTION_NAME"
echo "📋 Function type: $FUNCTION_TYPE"
echo "🏷️  Function category: $FUNCTION_CATEGORY"
echo "🔄 Function step: $FUNCTION_STEP"
echo "🖥️  Target node: $NODE_TYPE"
echo "🐳 Image: $IMAGE_NAME"

# Check if function directory exists
if [ ! -d "functions/$FUNCTION_NAME" ]; then
    echo "❌ Function directory functions/$FUNCTION_NAME not found"
    exit 1
fi

# Build function image if Dockerfile exists
if [ -f "functions/$FUNCTION_NAME/Dockerfile" ]; then
    echo "🔨 Building function image..."
    docker build -t $IMAGE_NAME:latest -f functions/$FUNCTION_NAME/Dockerfile functions/$FUNCTION_NAME/
    
    echo "📦 Loading image into Kind cluster..."
    kind load docker-image $IMAGE_NAME:latest --name serverless-cluster
else
    echo "⚠️  No Dockerfile found, using existing image: $IMAGE_NAME"
fi

# Create deployment YAML from template
echo "📝 Creating deployment configuration..."
DEPLOYMENT_FILE="k8s/deployments/${FUNCTION_NAME}.yaml"

mkdir -p k8s/deployments

# Replace placeholders in template
sed -e "s/FUNCTION_NAME/$FUNCTION_NAME/g" \
    -e "s/FUNCTION_TYPE/$FUNCTION_TYPE/g" \
    -e "s/FUNCTION_CATEGORY/$FUNCTION_CATEGORY/g" \
    -e "s/FUNCTION_STEP/$FUNCTION_STEP/g" \
    -e "s/FUNCTION_IMAGE/$IMAGE_NAME/g" \
    -e "s/FUNCTION_PATH/$FUNCTION_NAME/g" \
    -e "s/compute-node-1/$NODE_TYPE/g" \
    k8s/function-template.yaml > $DEPLOYMENT_FILE

# Deploy function
echo "🚀 Deploying function to Kubernetes..."
kubectl apply -f $DEPLOYMENT_FILE

# Wait for deployment to be ready
echo "⏳ Waiting for function to be ready..."
kubectl wait --for=condition=available deployment/$FUNCTION_NAME -n serverless --timeout=300s

echo "✅ Function $FUNCTION_NAME deployed successfully!"
echo ""
echo "🔍 Check status with:"
echo "   kubectl get pods -n serverless -l app=$FUNCTION_NAME"
echo "   kubectl logs -n serverless -l app=$FUNCTION_NAME"
echo "   kubectl describe pod -n serverless -l app=$FUNCTION_NAME"
echo ""
echo "🏷️  Function labels:"
echo "   app=$FUNCTION_NAME"
echo "   function-type=$FUNCTION_TYPE"
echo "   function-category=$FUNCTION_CATEGORY"
echo "   step=$FUNCTION_STEP"
echo ""
echo "📊 Monitor with:"
echo "   kubectl get pods -n serverless --show-labels"
echo "   kubectl get services -n serverless --show-labels"
