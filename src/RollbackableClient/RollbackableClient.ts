export default abstract class RollbackableClient {
    protected actions: {[id: string]: () => Promise<any>};
    protected reverseActions: {[id: string]: () => Promise<any>};
    protected transactionID: string;

    constructor(_transactionID: string){
        this.transactionID = _transactionID;
        this.actions = {};
        this.reverseActions = {};
    }

    public getTransactionID(): string {
        return this.transactionID;
    }

    public abstract invoke(tid: string): Promise<any>;
    public abstract rollback(tid: string): Promise<any>;
}