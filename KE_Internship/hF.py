"""
Честно взятый с гита
sklearn_hierarchical_classification
https://github.com/globality-corp/sklearn-hierarchical-classification
и переделанный под мои нужды
"""
import numpy as np

def h_precision_score(y_true, y_pred):
    """
    Parameters
    ----------
    y_true : array-like, shape = [n_samples, n_classes].
        разметка всех таргет классов по дереву для каждой записи. [[1,1,0,0,0,1,1],...]

    y_pred : array-like, shape = [n_samples, n_classes].
        разметка всех предсказанных классов по дереву для каждой записи. [[1,1,0,0,0,1,1],...]
    Returns
    -------
    hP : float
        посчитанный гармонически-взвешенный hierarchical precision score.

    """
    ix = np.where((y_true != 0) & (y_pred != 0))

    true_positives = len(ix[0])
    all_results = np.count_nonzero(y_pred)

    return true_positives / all_results


def h_recall_score(y_true, y_pred):
    """
    Parameters
    ----------
    y_true : array-like, shape = [n_samples, n_classes].
        разметка всех таргет классов по дереву для каждой записи. [[1,1,0,0,0,1,1],...]

    y_pred : array-like, shape = [n_samples, n_classes].
        разметка всех предсказанных классов по дереву для каждой записи. [[1,1,0,0,0,1,1],...]

    Returns
    -------
    hR : float
        посчитанный гармонически-взвешенный hierarchical recall score.

    """
    ix = np.where((y_true != 0) & (y_pred != 0))

    true_positives = len(ix[0])
    all_positives = np.count_nonzero(y_true)

    return true_positives / all_positives


def h_fbeta_score(y_true, y_pred, beta=1.):
    """
    Parameters
    ----------
    y_true : array-like, shape = [n_samples, n_classes].
        разметка всех таргет классов по дереву для каждой записи. [[1,1,0,0,0,1,1],...]

    y_pred : array-like, shape = [n_samples, n_classes].
        разметка всех предсказанных классов по дереву для каждой записи. [[1,1,0,0,0,1,1],...]

    beta: float
        beta parameter for the F-beta score. Defaults to F1 score (beta=1).

    Returns
    -------
    hFscore : float
        Посчитанный гармонически-взвешенный hierarchical F-score.

    """
    hP = h_precision_score(y_true, y_pred)
    hR = h_recall_score(y_true, y_pred)
    return (1. + beta ** 2.) * hP * hR / (beta ** 2. * hP + hR)
