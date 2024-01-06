import numpy as np
from sklearn.linear_model import LinearRegression
class LinReg(LinearRegression):

    encoding = {'Poor':0.0,
                'Poor To Fair':1.0,
                'Fair':2.0,
                'Fair To Good':3.0}

    def __init__(self):
        super().__init__()

    def fit(self , X, y, sample_weight = None):
        y = self.encode(y)
        return super().fit(X, y, sample_weight)

    def predict_labels(self, X):
        y =  np.round(super().predict(X))
        return self.decode(y)
    
    def score(self, X, y, sample_weight = None):
        y = self.encode(y)
        return super().score(X, y, sample_weight)
    
    def encode(self,arr):
        arr = arr.copy()
        try:
            arr = arr.values
        except AttributeError:
            pass
        
        for key,val in self.encoding.items():
            arr[arr==key] = val
        arr = arr.astype(int)
        return arr
    
    def decode(self,arr):
        arr = arr.copy()
        arr = arr.astype(str).reshape(-1)
        for key,val in self.encoding.items():
            arr[arr==str(val)] = key
        return arr