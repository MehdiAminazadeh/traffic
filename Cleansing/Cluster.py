import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
from scipy.spatial.distance import cdist
from contextlib import contextmanager

class Analysis:
    def __init__(self, dataRead:str) -> None:
         self.dataRead = pd.read_csv(dataRead)
        
    @contextmanager
    def modify(self):
        try:
            self.dataRead['Intersection'] = "#" + self.dataRead['Intersection'].astype(str)
            yield self.dataRead['Intersection']
        finally:
            self.dataRead['Intersection'] = self.dataRead['Intersection'].str.replace("#", "")

    def clustering(self, cluster:int) -> None:
        if cluster < 3:
            cluster = 3
        else:
            cluster = cluster
        k_means = KMeans(n_clusters=cluster)
        self.dataRead = self.dataRead.groupby('Intersection').agg(
            {'Traffic': 'sum'}
        ).reset_index()
        
        prediction = k_means.fit_predict(self.dataRead[['Intersection', 'Traffic']])
        self.dataRead['Cluster'] = prediction
       
        with self.modify():
            plt.figure(figsize=(15,10))
            plt.scatter(self.dataRead['Intersection'], 
                        self.dataRead['Traffic'], 
                        c=prediction)
            plt.xticks(rotation=90)
            plt.xlabel("Intersection")
            plt.ylabel('Traffic')
            
            return plt.show()
       
    def elbow(self) -> None:
        distortion = []
        inertia = []
        mapDist = {}
        mapInert = {}
        
        self.dataRead = self.dataRead.groupby('Intersection').agg(
            {'Traffic': 'sum'}
        ).reset_index()
        x1 = self.dataRead['Intersection']
        x2 = self.dataRead['Traffic']
        X_lst = np.array(list(zip(x1, x2))).reshape(len(x1), 2)
        K = range(1,10)
        
        for knum in K:
            kmeanModel = KMeans(n_clusters=knum).fit(X_lst)
            kmeanModel.fit(X_lst)
            distortion.append(sum(np.min(cdist(X_lst,
                                                 kmeanModel.cluster_centers_,
                                                 "euclidean"), axis=1)) / X_lst.shape[0])
            inertia.append(kmeanModel.inertia_)
            mapDist[knum] = sum(np.min(cdist(X_lst, kmeanModel.cluster_centers_,
                                               "euclidean"), axis=1)) / X_lst.shape[0]
            mapInert[knum] = kmeanModel.inertia_
        
        fig, (ax1,ax2) = plt.subplots(2, figsize=(10,10))
        fig.suptitle('Elbow method')
        ax1.plot(K, inertia, 'bx-')
        ax1.set_title('Based on Inertia')
        ax2.plot(K, distortion, 'bx-')
        ax2.set_title('Based on Distortion')
        
        try:
            import warnings
        except warnings:
            warnings.filterwarnings("ignore")
        return plt.show()
        
            
analyze = Analysis('output.csv')
analyze.elbow()
# analyze.Clustering(4)
analyze.clustering(7)
