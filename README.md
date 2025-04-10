# Argoverse 2 to Foxglove
This project lets you convert the [Argoverse 2](https://www.argoverse.org/av2.html) dataset to MCAP to be visualized in [Foxglove](https://foxglove.dev/). 

To get started, clone this repository and pip install the dependencies in `requirements.txt`. </br>
Then download the Argoverse dataset by following [this guide](https://argoverse.github.io/user-guide/getting_started.html) (specifically, download the logs found in "s3://argoverse/datasets/av2/sensor/train"). 

To convert a log to MCAP, execute the following command
```
python3 ./foxglove-av2/convert.py --data_root <path to dataset dir> --log_id <log id>
```
For example
```
python3 ./foxglove-av2/convert.py --data_root /home/alp/data/datasets/ --log_id 00a6ffc1-6ce9-3bc3-a060-6006e9893a1a
```
This will produce an MCAP file for each type of data (annotations, camera, lidar, pose). </br>
To combine the files into a single MCAP file, download the [MCAP command line tool ](https://mcap.dev/guides/cli) and use the `merge` command.