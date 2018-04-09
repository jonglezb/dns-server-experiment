from collections import namedtuple
import argparse

RateDuration = namedtuple("RateDuration", ["rate", "duration_ms"])

def parse_rateduration(s):
    rate, duration = s.split()
    if duration.endswith("ms"):
        d_ms = int(duration[:-2])
    elif duration.endswith("s"):
        d_ms = int(duration[:-1]) * 1000
    else:
        d_ms = int(duration)
    return RateDuration(rate=int(rate), duration_ms=d_ms)

def int_or_rates_duration(string):
    try:
        return int(string)
    except ValueError:
        # Complex format, e.g.: "100 8s, 200 5s, 300 400ms"
        try:
            ret = [parse_rateduration(x) for x in string.split(",")]
        except Exception as e:
            raise argparse.ArgumentTypeError("Invalid format: {}".format(e))
        return ret

def disable_pty(connection_params):
    """Given a set of execo connection parameters, disable the use of a PTY.

    The main advantage is that stdout and stderr of remote processes will
    no longer be mixed up.  Also, stdin will no longer be echoed back to
    stdout.

    """
    ssh_options = tuple(opt for opt in connection_params["ssh_options"] if opt != '-tt')
    connection_params["ssh_options"] = ssh_options
    connection_params["pty"] = False
