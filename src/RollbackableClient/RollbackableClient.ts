export interface RollbackableAction {
    action: () => Promise<any>;
    rollbackAction: () => Promise<any>;
} 

// Todo: add genrics
export default abstract class RollbackableClient {
    protected actions: Map<string, RollbackableAction>;
    protected rollbackActions: Map<string, () => Promise<any>>;
    protected transactionID: string;

    constructor(_transactionID: string){
        this.transactionID = _transactionID;
        this.actions = new Map<string,RollbackableAction>();
        this.rollbackActions = new Map<string, () => Promise<any>>();
    }

    public getTransactionID(): string {
        return this.transactionID;
    }

    
    public abstract invoke(): Promise<boolean>;
    public abstract rollback(): Promise<any>;
}