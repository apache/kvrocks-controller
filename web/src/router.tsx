import { RouteObject } from 'react-router-dom';
import { Namespace } from './pages/Namespace';
import { Cluster } from './pages/Cluster';

export const router: RouteObject[] = [
    {
        path: '/',
        element: <></>
    },{
        path: '/:namespace',
        element: <Namespace/>
    },{
        path: '/:namespace/:cluster',
        element: <Cluster/>
    }
];