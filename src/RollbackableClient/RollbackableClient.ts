export default abstract class RollbackableClient {
    protected actions: {[id: string]: () => Promise<any>};
    protected reverseActions: {[id: string]: () => Promise<any>};

    constructor(){
        this.actions = {};
        this.reverseActions = {};
    }

    public abstract invoke(tid: string): Promise<any>;
    public abstract rollback(tid: string): Promise<any>;
}