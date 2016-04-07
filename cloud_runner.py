import cloud_continious_monitoring.util as ccm_util
import sys


def main_func():
    name = sys.argv[1] if len(sys.argv) > 1 else None

    if name:
        ccm_util.say_hi(name)
    else:
        print('Please supply a name')
