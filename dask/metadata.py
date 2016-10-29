from __future__ import absolute_import, division, print_function

from contextlib import contextmanager
import sys
import traceback

from .base import Base
from .utils import funcname


def _emulate(func, *args, **kwargs):
    """
    Apply a function using args / kwargs. If arguments contain dd.DataFrame /
    dd.Array, using internal cache (``_meta``) for calculation
    """
    with raise_on_meta_error(funcname(func)):
        return func(*_extract_meta(args, True), **_extract_meta(kwargs, True))


def _extract_meta(x, nonempty=False):
    """
    Extract internal cache data (``_meta``) from dd.DataFrame / dd.Series / dd.Array
    """
    if isinstance(x, Base):
        return x._meta_nonempty if nonempty else x._meta
    elif isinstance(x, list):
        return [_extract_meta(_x, nonempty) for _x in x]
    elif isinstance(x, tuple):
        return tuple([_extract_meta(_x, nonempty) for _x in x])
    elif isinstance(x, dict):
        res = {}
        for k in x:
            res[k] = _extract_meta(x[k], nonempty)
        return res
    else:
        return x


@contextmanager
def raise_on_meta_error(funcname=None):
    """Reraise errors in this block to show metadata inference failure.

    Parameters
    ----------
    funcname : str, optional
        If provided, will be added to the error message to indicate the
        name of the method that failed.
    """
    try:
        yield
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb = ''.join(traceback.format_tb(exc_traceback))
        msg = ("Metadata inference failed{0}.\n\n"
               "Original error is below:\n"
               "------------------------\n"
               "{1}\n\n"
               "Traceback:\n"
               "---------\n"
               "{2}"
               ).format(" in `{0}`".format(funcname) if funcname else "",
                        repr(e), tb)
        raise ValueError(msg)
