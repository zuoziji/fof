import pickle
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import probscale

with open('tl.pkl','rb') as f:

    tl = pickle.load(f)
    #print(tl.T)

    plt.style.use('fivethirtyeight')
    ax = tl.plot()
    plt.show()