docker build --no-cache -t workflow_video_base ../benchmark/template_functions/video__base
docker build --no-cache -t video__upload ../benchmark/template_functions/video__upload
docker build --no-cache -t video__split ../benchmark/template_functions/video__split
docker build --no-cache -t video__group0 ../benchmark/template_functions/video__group0
docker build --no-cache -t video__transcode ../benchmark/template_functions/video__transcode
docker build --no-cache -t video__merge ../benchmark/template_functions/video__merge
docker build --no-cache -t video__simple_process ../benchmark/template_functions/video__simple_process

