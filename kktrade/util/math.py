import numpy as np
# local package
from kkpsgre.util.com import check_type_list, check_type


__all__ = [
    "NonLinearXY"
]


class NonLinearXY:
    def __init__(self, bins_x: np.ndarray | list[float], bins_y: np.ndarray | list[float], outlier: float=None, is_ignore_nan: bool=False):
        """
        Usage::
            >>> from xxxxxxx.util.math import NonLinearXY
            >>> lin = NonLinearXY([0.0, 0.2, 1.0], [1.1, 2.2, 1.5])
            >>> lin(0.3)
            2.1125000000000003
            >>> lin(0.1)
            1.6500000000000001
            >>> lin(-1)
            1.1
            >>> lin(5)
            1.5
        """
        assert isinstance(bins_x, np.ndarray) or check_type_list(bins_x, float) and len(bins_x) >= 2
        assert isinstance(bins_y, np.ndarray) or check_type_list(bins_y, float) and len(bins_y) >= 2
        assert len(bins_x) == len(bins_y)
        if outlier is not None: assert isinstance(outlier, float)
        assert isinstance(is_ignore_nan, bool)
        for i, x in enumerate(bins_x[:-1]):
            assert x < bins_x[i+1]
        if is_ignore_nan:
            ndf_bool = np.logical_not(np.isnan(bins_y))
            bins_x   = np.array(bins_x)[ndf_bool]
            bins_y   = np.array(bins_y)[ndf_bool]
            if bins_x.shape[0] == 0:
                # It means All-nan.
                bins_x = np.array([0.0,])
                bins_y = np.array([0.0,])
        else:
            assert np.isnan(bins_y).sum() == 0
        self.bins_x  = bins_x.copy() if isinstance(bins_x, np.ndarray) else np.array(bins_x).copy()
        self.bins_y  = bins_y.copy() if isinstance(bins_x, np.ndarray) else np.array(bins_y).copy()
        self.x_min   = self.bins_x.min()
        self.x_max   = self.bins_x.max()
        self.y_left  = self.bins_y[0 ] if outlier is None else outlier
        self.y_right = self.bins_y[-1] if outlier is None else outlier
    def __call__(self, x: int | float | np.ndarray, return_nan_to_value: float=float("nan")):
        assert type(return_nan_to_value) in [int, float, np.int8, np.int16, np.int32, np.int64, np.float16, np.float32, np.float64]
        if isinstance(x, np.ndarray):
            return self.call_numpy(x, return_nan_to_value=return_nan_to_value)
        elif check_type(x, [int, float]):
            return self.call_normal(x, return_nan_to_value=return_nan_to_value)
        else:
            raise ValueError(f"type: {type(x)} is not matched.")
    def call_normal(self, x: float, return_nan_to_value: float=float("nan")):
        if x == float("nan"):         return return_nan_to_value
        if self.bins_x.shape[0] == 1: return return_nan_to_value
        if   self.x_max <= x:         return self.y_right
        elif self.x_min >= x:         return self.y_left
        id_min = np.where(self.bins_x <= x)[0].max()
        id_max = np.where(self.bins_x >  x)[0].min()
        x1, x2 = self.bins_x[id_min], self.bins_x[id_max]
        y1, y2 = self.bins_y[id_min], self.bins_y[id_max]
        ratio  = (x - x1) / (x2 - x1)
        return y1 + ratio * (y2 - y1)
    def call_numpy(self, ndf: np.ndarray, return_nan_to_value: float=float("nan")):
        output  = np.ones_like(ndf) * return_nan_to_value
        if self.bins_x.shape[0] == 1: return output
        boolnan = np.isnan(ndf)
        for i in np.arange(len(self.bins_x) - 1, dtype=int):
            bool_bins = (self.bins_x[i] <= ndf) & (ndf < self.bins_x[i+1])
            bool_bins = (bool_bins & np.logical_not(boolnan))
            ndf_ratio = (ndf[bool_bins] - self.bins_x[i]) / (self.bins_x[i+1] - self.bins_x[i])
            output[bool_bins] = (self.bins_y[i] + ndf_ratio * (self.bins_y[i+1] - self.bins_y[i]))
        output[(self.bins_x[0]  >  ndf)] = self.y_left
        output[(self.bins_x[-1] <= ndf)] = self.y_right
        output[boolnan] = return_nan_to_value
        return output
    def is_higher(self, x: float | np.ndarray, y: float | np.ndarray):
        val = self(x)
        return (val <= y)