# CloudPipe

Lightweight python tool for creating process steps in a data pipeline allowing code to seamlessly run in cloud/local environments.
Works as __AWS Lambda__ function with __S3__ data storage. 
More cloud options TBD.

Ideal for use with orchestration methods viz. __AWS Step Functions__

```python
from cloudpipe import Step, MAP_RETURN

cloudmap = Step(
    location_env_key=dict(s3="ENV_DATA_BUCKET"), 
    local=dict(local="/path/to/root"))


@cloudmap(
    source={
        "upload": "{doc}.jpg"
    }, 
    destn={
        "collage": "{doc}/combined_faces.jpeg",
        "faces": "{doc}/faces/*"
    }, 
    list_copy_keys=['document'])

def worker_detect_faces(upload: Path, collage: Path, faces: Path, 
                        prep_retn: MAP_RETURN = None,
                        upload_time = None,
                        faces_args = None, **kwargs):
    
    detect_faces(upload, save_to=faces)
    make_collage(faces, save_to=collage)

```

__Cloud__ invoke:

```json
{
    "document": {
        "name": "1"
    },
    "upload": {
        "Key": "uploaded/bob.jpeg",
        "time": ""
    }
}
```

Response:
```json
{
    "document": 
    {
        "name": "1"
    },
    "upload": 
    {
        "Key": "uploaded/bob.jpeg"
    },
    "collage": 
    {
        "Key": "collage/1/combined_faces.jpeg"
    },
    "faces": [
        {
            "document": {"name": 1},
            "faces": {"Key": "faces/1/faces/0.jpeg"}
        },
        {
            "document": {"name": 1},
            "faces": {"Key": "faces/1/faces/1.jpeg"}
        }
    ]
}
```

__Local__ invoke:
```python
input_path = Path("input_image.jpeg")
collage = Path("save/collage.jpeg")
faces = Path("save/faces")

worker_detect_faces(upload=input_path, collage=collage, faces=faces)
```