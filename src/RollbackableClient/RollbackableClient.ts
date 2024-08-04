export default abstract class RollbackableClient {
    private reverseActions: {[id: string]: () => Promise<any>};

    constructor(){
        this.reverseActions = {};
    }

    abstract rollback(actionID: string): Promise<any>;
}