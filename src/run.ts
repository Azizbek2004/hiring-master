import { IExecutor } from "./Executor";
import ITask from "./Task";

export default async function run(executor: IExecutor, queue: AsyncIterable<ITask>, maxThreads = 0) {
    maxThreads = Math.max(0, maxThreads);
    const runningTasks = new Map<number, Promise<void>>();
    const taskQueue: ITask[] = [];

    for await (const task of queue) {
        taskQueue.push(task);
    }

    const executeTask = async (task: ITask) => {
        await executor.executeTask(task);
        runningTasks.delete(task.targetId);
    };

    while (taskQueue.length > 0) {
        while (runningTasks.size < maxThreads || maxThreads === 0) {
            const task = taskQueue.shift();
            if (!task) break;

            if (!runningTasks.has(task.targetId)) {
                runningTasks.set(task.targetId, executeTask(task));
            } else {
                taskQueue.unshift(task); // Re-add the task to the front of the queue
                break; // Wait for a running task to complete
            }
        }

        await Promise.race(runningTasks.values());
    }

    await Promise.all(runningTasks.values());
}