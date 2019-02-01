import glob
import datetime
import numpy as np
import pandas as pd
from numpy import mean, absolute
from scipy.stats import iqr, kurtosis
import scipy.fftpack
from scipy import signal
from scipy.ndimage.filters import gaussian_filter
import scipy.fftpack
from scipy.stats import entropy

def mad(data, axis=None):
    return mean(absolute(data - mean(data, axis)), axis)

def time_features_extract(X, w_size):
    row =  len(X)//w_size
    col = w_size
    X_w = X.reshape(row, col)
    max = np.amax(X_w, axis=1)
    min = np.amin(X_w, axis=1)
    mean = np.mean(X_w, axis=1)
    std = np.std(X_w, axis=1)
    
    #interquartile range
    itr = iqr(X_w, axis=1)

    #kurtosis
    kurt = kurtosis(X_w, axis=1)
    
    #variance
    var = np.var(X_w, axis=1)
    
    return np.vstack((max, min, mean, std, itr, kurt, var)).T

def run_fft(feature, n=100):
    number_of_runs = feature.shape[0] // n
    
    amp_spec_list = []
    
    for run in range(number_of_runs):
        a = n * run
        b = n * (run + 1)
        hanning_window = np.hanning(n)
        X = scipy.fftpack.fft(hanning_window * feature[a:b])
        amp_spec = [np.sqrt(c.real ** 2 + c.imag ** 2) for c in X]
        amp_spec_list.append(amp_spec[0:50])
        
    fft = np.array(amp_spec_list)
    
    print(fft.shape)
        
    return fft

def filtered_fft(feature, N):
    b1, a1 = signal.butter(5, [0.02, 0.95], "bandpass")
    butter = signal.filtfilt(b1, a1, feature)

    fft = run_fft(butter, n=N)
    med = signal.medfilt(fft, 3)
    gaus = gaussian_filter(med, sigma=1.2)
    
    return gaus

def freq_features_extract(X):
    row = len(X)
    highest_magnitude = np.amax(X, axis=1).reshape(row, 1)
    highest_magnitude_located = np.argmax(X, axis=1).reshape(row, 1)
    ent = entropy(X.T, base=2).reshape(row, 1)
    total_energy = np.sum(X, axis=1)
    subband_energy = np.sum(X.reshape(row, 10, 5), axis=2)
    ratio_subband_energy = (subband_energy.T / total_energy).T
    subband_ent = entropy(X.reshape(row, 10, 5).T, base=2).T
    
    X_log = 20 * np.log10(X)
    ori_cps = scipy.fftpack.realtransforms.dct(X_log, type=2, norm="ortho", axis=-1)
    cps =  ori_cps[:, 1:21]
    diff_cps = (ori_cps[:, 0:20] - ori_cps[:, 1:21])
    
    temp = np.hstack((highest_magnitude, highest_magnitude_located, ent, total_energy.reshape(row, 1), subband_energy, ratio_subband_energy, subband_ent, cps, diff_cps))
    return temp

def generate_unique_ID():
  return datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')

def setRedisKV(r, K, V):
  try:
    # r = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)
    r.set(K, V)
    return True
  except:
    return False

def getRedisV(r, K):
  # r = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)
  output = r.get(K)
  if output is not None:
    return output
  else:
    return "Empty"

def appendToListK(r, K, V):
  try:
    # r = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)
    r.rpush(K, V)
    return True
  except:
    return False

def incrRedisKV(r, K):
  try:
    # r = redis.StrictRedis(host="redis", port=6379, password="", decode_responses=True)
    r.incr(K)
    return True
  except:
    return False

def getListK(r, K):
  output = r.lrange(K, 0, -1)
  if output is not None:
    return output
  else:
    return "Empty"


