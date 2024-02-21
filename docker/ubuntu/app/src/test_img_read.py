from cv2 import imdecode, IMREAD_COLOR, imshow, waitKey
import urllib.request
import numpy as np

stream = urllib.request.urlopen('http://211.132.61.124:80/mjpg/video.mjpg')
bytes = bytes()
while True:
    bytes += stream.read(1024)
    a = bytes.find(b'\xff\xd8')
    b = bytes.find(b'\xff\xd9')
    if a != -1 and b != -1:
        jpg = bytes[a:b+2]
        bytes = bytes[b+2:]
        i = imdecode(np.fromstring(jpg, dtype=np.uint8), IMREAD_COLOR)
        imshow('i', i)
        if waitKey(1) == 27:
            exit(0)