import { useEffect, useState } from 'react';
import { Cluster } from '../entitits/Cluster';
import axios, {AxiosResponse} from 'axios';
import { message } from 'antd';
import { sleep } from '../common/utils';

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

type ResponseBody<T extends ApiType> = T extends 'listNamespace' ? string[] :
                                       T extends 'createNamespace' ? boolean :
                                       T extends 'deleteNamespace' ? boolean :
                                       T extends 'listCluster' ? string[] :
                                       T extends 'createCluster' ? boolean :
                                       T extends 'getCluster' ? Cluster :
                                       T extends 'deleteCluster' ? boolean :
                                       undefined;

axios.defaults.baseURL = `${window.location.origin}/api/v1`;

function getErrorMessageFromResponse(response: AxiosResponse) {
    let errMsg = '';
    if(typeof response.data == 'object' && response.data != null && 'error' in response.data) {
        if('message' in response.data['error']) {
            errMsg = response.data['error']['message'];
        } else {
            errMsg = 'Unknown error';
        }
    }
    return errMsg;
}

export async function sendRequest<T extends ApiType>(type: T, body: RequestBody<T>): Promise<{
    response: ResponseBody<T>,
    errorMessage: string
}> {
    // await sleep(2000);
    let url: string;
    let method: '' | 'GET' | 'POST' | 'DELETE';
    let requestBody;
    let getResponseData: (res: any) => ResponseBody<T>;
    switch (type) {
    case 'listNamespace':{
        method = 'GET';
        url = '/namespaces';
        getResponseData = (res) => Array.isArray(res.data.namespaces) ? res.data.namespaces : [];
        break;
    }
    case 'createNamespace': {
        method = 'POST';
        url = '/namespaces';
        requestBody = {
            namespace: body
        };
        getResponseData = (res) => (res?.data == 'created') as ResponseBody<T>;
        break;
    }
    case 'listCluster': {
        method = 'GET';
        url = `/namespaces/${body}/clusters`;
        getResponseData = (res) => Array.isArray(res.data.clusters) ? res.data.clusters : [];
        break;
    }
    default:
        method = '';
        url = '';
        getResponseData = () => undefined as ResponseBody<T>;
        break;
    }
    if (!method || !url) {
        const msg = 'No method or url configured, please check your parameter';
        console.error(msg);
        throw new Error(msg);
    }
    try {
        const response = await axios({
            method,
            url,
            data: requestBody
        });
        const errMsg = getErrorMessageFromResponse(response);
        const result = getResponseData(response.data);
        return {
            response: result,
            errorMessage: errMsg
        };

    } catch (error) {
        let errMsg = '';
        if(error instanceof axios.AxiosError) {
            if (typeof error.response?.data == 'object') {
                errMsg = getErrorMessageFromResponse(error.response);
            } else if(typeof error.response?.data == 'string') {
                errMsg = error.response.data;
            } else if (typeof error.message == 'string') {
                errMsg = error.message;
            } else {
                errMsg = 'Unknown error';
            }
        } else if(error instanceof Error) {
            errMsg = error.message;
        } else {
            if (typeof error == 'string') {
                errMsg = error;
            } else if(typeof error == 'object') {
                errMsg = JSON.stringify(error);
            } else {
                errMsg = 'Unknown error';
            }
        }
        return {
            response: undefined as ResponseBody<T>,
            errorMessage: errMsg
        };
    }
}

export function useApi<T extends ApiType>(type: T, body: RequestBody<T>, autoHandleErrorMessage = true):[
    boolean,
    ResponseBody<T> | undefined,
    string,
    () => void
] {
    const [loading, setLoading] = useState(true);
    const [response, setResponse] = useState<ResponseBody<T>>();
    const [errorMessage, setErrorMessage] = useState<string>('');
    const [refreshCount, setRefreshCount] = useState(0);
    useEffect(() => {
        (async () => {
            const res = await sendRequest(type, body);
            setLoading(false);
            if(res.errorMessage) {
                if(autoHandleErrorMessage) {
                    message.error({
                        content: res.errorMessage
                    });
                } else {
                    setErrorMessage(res.errorMessage);
                }
            } else {
                setResponse(res.response);
            }
        })();
    },[refreshCount]);
    return [loading, response, errorMessage, () => setRefreshCount(c => c + 1)];
}