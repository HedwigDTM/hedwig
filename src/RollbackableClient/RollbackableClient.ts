export default abstract class RollbackableClient {
    protected actions: Map<string, () => Promise<any>>;
    protected reverseActions: Map<string, () => Promise<any>>;
    protected transactionID: string;

    constructor(_transactionID: string){
        this.transactionID = _transactionID;
        this.actions = new Map<string, () => Promise<any>>();
        this.reverseActions = new Map<string, () => Promise<any>>();
    }

    public getTransactionID(): string {
        return this.transactionID;
    }

    public abstract invoke(tid: string): Promise<any>;
    public abstract rollback(tid: string): Promise<any>;
}