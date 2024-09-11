import numpy as np
# local package
from kkpsgre.util.com import check_type_list, check_type


__all__ = [
    "NonLinearXY"
]


class NonLinearXY:
    def __init__(self, bins_x: list[float], bins_y: list[float], outlier: float=None):
        """
        Usage::
            >>> from boatracebot.util.math import NonLinearXY
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
        assert isinstance(bins_x, np.ndarray) or check_type_list(bins_x, float)
        assert isinstance(bins_y, np.ndarray) or check_type_list(bins_y, float)
        assert len(bins_x) == len(bins_y)
        if outlier is not None: assert isinstance(outlier, float)
        for i, x in enumerate(bins_x[:-1]):
            assert x < bins_x[i+1]
        self.bins_x  = bins_x.copy() if isinstance(bins_x, np.ndarray) else np.array(bins_x).copy()
        self.bins_y  = bins_y.copy() if isinstance(bins_x, np.ndarray) else np.array(bins_y).copy()
        self.x_min   = self.bins_x.min()
        self.x_max   = self.bins_x.max()
        self.y_left  = self.bins_y[0 ] if outlier is None else outlier
        self.y_right = self.bins_y[-1] if outlier is None else outlier
    def __call__(self, x: int | float | np.ndarray):
        if isinstance(x, np.ndarray):
            return self.call_numpy(x)
        elif check_type(x, [int, float]):
            return self.call_normal(x)
        else:
            raise ValueError(f"type: {type(x)} is not matched.")
    def call_normal(self, x: float):
        if   self.x_max <= x: return self.y_right
        elif self.x_min >= x: return self.y_left
        id_min = np.where(self.bins_x <= x)[0].max()
        id_max = np.where(self.bins_x >  x)[0].min()
        x1, x2 = self.bins_x[id_min], self.bins_x[id_max]
        y1, y2 = self.bins_y[id_min], self.bins_y[id_max]
        ratio  = (x - x1) / (x2 - x1)
        return y1 + ratio * (y2 - y1)
    def call_numpy(self, ndf: np.ndarray):
        output = np.zeros_like(ndf)
        for i in np.arange(len(self.bins_x) - 1, dtype=int):
            bool_bins = (self.bins_x[i] <= ndf) & (ndf < self.bins_x[i+1])
            ndf_ratio = (ndf[bool_bins] - self.bins_x[i]) / (self.bins_x[i+1] - self.bins_x[i])
            output[bool_bins] = (self.bins_y[i] + ndf_ratio * (self.bins_y[i+1] - self.bins_y[i]))
        output[(self.bins_x[0]  >  ndf)] = self.y_left
        output[(self.bins_x[-1] <= ndf)] = self.y_right
        return output
    def is_higher(self, x: float | np.ndarray, y: float | np.ndarray):
        val = self(x)
        return (val <= y)