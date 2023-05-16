import { useCallback, useEffect, useState } from 'react';
import { Cluster } from '../entitits/Cluster';
import axios, {AxiosResponse} from 'axios';
import { message } from 'antd';

type ApiType = 'listNamespace'
| 'createNamespace'
| 'deleteNamespace'
| 'listCluster'
| 'createCluster'
| 'getCluster'
| 'deleteCluster'

type RequestBody<T extends ApiType> = {
    'listNamespace': undefined;
    'createNamespace': string;
    'deleteNamespace': string;
    'listCluster': string;
    'createCluster': Cluster;
    'getCluster': {namespace: string, cluster: string};
    'deleteCluster': {namespace: string, cluster: string}
}[T]

type ResponseBody<T extends ApiType> = {
    'listNamespace': string[],
    'createNamespace': boolean;
    'deleteNamespace': boolean;
    'listCluster': string[];
    'createCluster': boolean;
    'getCluster': Cluster;
    'deleteCluster': boolean
}[T]

axios.defaults.baseURL = `${window.location.origin}/api/v1`;
axios.defaults.validateStatus = () => true;

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

async function sendRequest<T extends ApiType>(type: T, body: RequestBody<T>): Promise<{
    response: ResponseBody<T>,
    errorMessage: string
}> {
    let url: string;
    let method: '' | 'GET' | 'POST' | 'DELETE';
    let requestBody;
    let getResponseData: (res: any) => ResponseBody<T>;
    switch (type) {
    case 'listNamespace':{
        method = 'GET';
        url = '/namespaces';
        getResponseData = (res) => Array.isArray(res?.data?.namespaces) ? res.data.namespaces : [];
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
    case 'deleteNamespace': {
        method = 'DELETE';
        url = `/namespaces/${body}`;
        getResponseData = (res) => (res?.data == 'ok') as ResponseBody<T>;
        break;
    }
    case 'listCluster': {
        method = 'GET';
        url = `/namespaces/${body}/clusters`;
        getResponseData = (res) => Array.isArray(res?.data?.clusters) ? res.data.clusters : [];
        break;
    }
    case 'createCluster': {
        method = 'POST';
        url = `/namespaces/${(body as RequestBody<'createCluster'>).namespace}/clusters`;
        requestBody = (body as RequestBody<'createCluster'>).getCreationBody();
        getResponseData = res => (res?.data == 'created') as ResponseBody<T>;
        break;
    }
    case 'getCluster': {
        method = 'GET';
        const {namespace, cluster} = (body as RequestBody<'getCluster'>);
        url = `/namespaces/${namespace}/clusters/${cluster}`;
        getResponseData = res => {
            const clusterEntity = new Cluster();
            clusterEntity.parseFromResponse(typeof res?.data?.cluster == 'object' ? res.data.cluster : {});
            clusterEntity.namespace = namespace;
            return clusterEntity as ResponseBody<T>;
        };
        break;
    }
    case 'deleteCluster': {
        method = 'DELETE';
        const {namespace, cluster} = (body as RequestBody<'deleteCluster'>);
        url = `/namespaces/${namespace}/clusters/${cluster}`;
        getResponseData = res => (res?.data == 'ok') as ResponseBody<T>;
        break;
    }
    default:
        method = '';
        url = '';
        getResponseData = () => undefined as any;
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
            response: undefined as any,
            errorMessage: errMsg
        };
    }
}

export function useApi<T extends ApiType>(
    type: T,
    body?: RequestBody<T>,
    autoHandleErrorMessage = true,
    callWhenInit = false,
):{
    loading: boolean,
    response: ResponseBody<T> | undefined,
    errorMessage: string,
    send: (body: RequestBody<T>) => Promise<ResponseBody<T>>
} {
    const [loading, setLoading] = useState(false);
    const [response, setResponse] = useState<ResponseBody<T>>();
    const [errorMessage, setErrorMessage] = useState<string>('');
    let requestBody:RequestBody<T>;
    if(body !== undefined) {
        requestBody = body;
    }
    const send = useCallback(async () => {
        setLoading(true);
        const res = await sendRequest(type, requestBody);
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
        return res.response;
    },[]);
    useEffect(() => {
        callWhenInit && send();
    },[callWhenInit]);
    const refresh = useCallback(async (body: RequestBody<T>): Promise<ResponseBody<T>> => {
        requestBody = body;
        return send();
    },[]);
    return {
        loading,
        response,
        errorMessage,
        send: refresh
    };
}