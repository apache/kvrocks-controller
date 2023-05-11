export class Cluster {
    name: string;
    namespace: string;
    nodes: string[];
    password: string|null;
    replicas: number|null;
    constructor(data?: object) {
        this.name = '';
        this.namespace = '';
        this.nodes = [];
        this.password = null;
        this.replicas = null;
        if(data) {
            if('name' in data && typeof data.name == 'string') {
                this.name = data.name;
            }
            if('namespace' in data && typeof data.namespace == 'string') {
                this.namespace = data.namespace;
            }
            if('nodes' in data && Array.isArray(data.nodes)) {
                this.nodes = data.nodes.filter(n => typeof n == 'string');
            }
            if('password' in data && typeof data.password == 'string') {
                this.password = data.password;
            }
            if('replicas' in data && typeof data.replicas == 'number') {
                this.replicas = Math.round(data.replicas);
            }
        }
    }
    getCreationBody(): object {
        const result: any = {
            name: this.name,
            nodes: this.nodes
        };
        if(this.password != null) {
            result.password = this.password;
        }
        if(this.replicas != null) {
            result.replicas = this.replicas;
        }
        return result;
    }
}