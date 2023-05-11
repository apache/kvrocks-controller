import { useParams } from 'react-router-dom';

export function Cluster() {
    const {namespace, cluster} = useParams();
    return (<>
        {
            namespace && cluster && <div>
                this is cluster page, {namespace} {cluster}
            </div>
        }
    </>);
}