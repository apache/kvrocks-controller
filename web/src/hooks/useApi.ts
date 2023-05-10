import { useState } from 'react';
import { Cluster } from '../entitits/Cluster';
import axios from 'axios';

type ApiType = 'listNamespace'
| 'createNamespace'
| 'deleteNamespace'
| 'listCluster'
| 'createCluster'
| 'getCluster'
| 'deleteCluster'

type RequestBody<T extends ApiType> = T extends 'createNamespace' ? string :
                                      T extends 'deleteNamespace' ? string :
                                      T extends 'listCluster' ? string :
                                      T extends 'createCluster' ? {namespace: string, body: Cluster} :
                                      T extends 'getCluster' ? {namespace: string, cluster: string} :
                                      T extends 'deleteCluster' ? {namespace: string, cluster: string} :
                                      undefined;

axios.defaults.baseURL = 'http://localhost:9379/api/v1';

export function useApi<T extends ApiType>(type: T, body: RequestBody<T>) {
    const [loading, setLoading] = useState(true);
    let url: string;
    let method: string;
    switch (type) {
    case 'listNamespace':{
        method = 'GET';
        url = '/namespaces';
        break;
    }
    default:
        method = '';
        url = '';
        break;
    }
    if (!method || !url) {
        const msg = 'No method or url configured, please check your parameter';
        console.error(msg);
        throw new Error(msg);
    }
    axios({
        method,
        url
    });
    return [loading];
}