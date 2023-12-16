from time import perf_counter
from functools import wraps


def performance(fn):
    '''
    This decorator measures how long it takes to execute a function.
    '''

    @wraps(fn)
    def wrapper(*args, **kwargs):
        start = perf_counter()
        result = fn(*args, **kwargs)
        stop = perf_counter()
        print(f"Execution time of {fn.__name__}: {stop - start} seconds")
        return result

    return wrapper


def debug(fn):
    '''
    This decorator is useful for debugging purposes,
    as it prints the name, arguments, and return value of the function it wraps.
    '''

    @wraps(fn)
    def wrapper(*args, **kwargs):
        print(f"Calling {fn.__name__} with args: {args} and kwargs: {kwargs}")
        result = fn(*args, **kwargs)
        print(f"{fn.__name__} returned: {result}")
        return result

    return wrapper


def memoize(func):
    '''
    This decorator is useful for optimizing the performance of recursive or expensive functions,
    as it caches the results of previous calls and returns them if the same arguments are passed again.
    '''
    cache = {}

    @wraps(func)
    def wrapper(*args):
        result = cache.get(args)
        if result is None:
            result = func(*args)
            cache[args] = result
        return result

    return wrapper
