_patched = False
def patch():
    '''
    Should be called before using deep_genome.core
    
    Applied patches:
    
    - monkey patch these issues (if not fixed yet): https://github.com/pydata/pandas/issues/8222
    
    This function is idempotent. I.e. any calls but the first to `patch` are ignored. 
    '''
    global _patched
    if _patched:
        return
    
    import pandas as pd
    from functools import wraps
    
    # Fix https://github.com/pydata/pandas/issues/8222 which releases aug 2017 with 0.19.x
    if tuple(map(int, pd.__version__.split('.')[0:1])) < (0, 19):
        applymap_ = pd.DataFrame.applymap
        @wraps(applymap_)
        def monkey_patch(self, *args, **kwargs):
            if self.empty:
                return self
            return applymap_(self, *args, **kwargs)
        pd.DataFrame.applymap = monkey_patch
    
    _patched = True

__version__ = '0.0.0'  # Auto generated by ct-mksetup, do not edit this line. Project version is only set nonzero on release, using `ct-release`.