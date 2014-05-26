"""decorator: useful generic decorators"""


from abc import ABCMeta


class abstractclass(object):
    
    """Creates abstract classes from legacy ones."""
    
    def __init__(self, meta=ABCMeta):
        self.meta = meta
        
    def __call__(self, class_):
        """Build and return the very same class,
        but with abstract properties.
        """
        return self.meta(class_.__name__, class_.__mro__[1:],
                         dict(class_.__dict__))
