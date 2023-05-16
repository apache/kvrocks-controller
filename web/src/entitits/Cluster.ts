export class Cluster {
    name: string;
    namespace: string;
    nodes: Node[];
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
                this.nodes = data.nodes.filter(n => typeof n == 'string').map(n => new Node(n));
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
            nodes: this.nodes.map(n => n.addr)
        };
        if(this.password != null) {
            result.password = this.password;
        }
        if(this.replicas != null) {
            result.replicas = this.replicas;
        }
        return result;
    }
    parseFromResponse(obj: any) {
        if(typeof obj != 'object') {
            return;
        }
        if(typeof obj.name == 'string') {
            this.name = obj.name;
        }
        if(Array.isArray(obj.shards)) {
            this.nodes = obj.shards.map((item: any) => {
                const node = new Node();
                node.parseFromResponse(item);
                return node;
            });
        }
    }
}

class Node {
    id: string;
    addr: string;
    role: string;
    password: string;
    slotRanges: string[];
    constructor(addr?: string) {
        this.id = '';
        this.addr = '';
        this.role = '';
        this.password = '';
        this.slotRanges = [];
        if(typeof addr == 'string') {
            this.addr = addr;
        }
    }
    parseFromResponse(obj: any) {
        if(typeof obj != 'object') {
            return;
        }
        if(Array.isArray(obj.nodes) && obj.nodes.length > 0 && typeof obj.nodes[0] == 'object') {
            (typeof obj.nodes[0].id == 'string') && (this.id = obj.nodes[0].id);
            (typeof obj.nodes[0].addr == 'string') && (this.addr = obj.nodes[0].addr);
            (typeof obj.nodes[0].role == 'string') && (this.role = obj.nodes[0].role);
            (typeof obj.nodes[0].password == 'string') && (this.password = obj.nodes[0].password);
        }
        if(Array.isArray(obj.slot_ranges) && obj.slot_ranges.length > 0) {
            this.slotRanges = obj.slot_ranges.filter((item: any) => typeof item == 'string');
        }
    }
}