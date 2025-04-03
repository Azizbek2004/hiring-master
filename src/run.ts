import { IExecutor } from "./Executor";
import ITask from "./Task";

export default async function run(
  executor: IExecutor,
  queue: AsyncIterable<ITask>,
  maxThreads = 0
): Promise<void> {
  // Normalize maxThreads (0 means unlimited)
  maxThreads = Math.max(0, maxThreads);

  // Track running tasks by targetId (using number keys to match task.targetId)
  const runningTasks = new Map<number, Promise<void>>();
  // Queue for tasks that can't start yet
  const pendingTasks: ITask[] = [];
  const iterator = queue[Symbol.asyncIterator]();
  let queueExhausted = false;

  // Execute a task and handle its completion
  const executeTask = async (task: ITask) => {
    const promise = executor.executeTask(task);
    runningTasks.set(task.targetId, promise); // Use targetId directly as a number
    try {
      await promise;
      // After completion, check for more tasks to maintain concurrency
      scheduleNextTask();
    } catch (error) {
      console.error(`Error executing task ${task.targetId}:`, error);
    } finally {
      runningTasks.delete(task.targetId); // Clean up using the same number key
    }
  };

  // Check if a task can be started
  const canStartTask = (task: ITask): boolean => {
    return (
      !runningTasks.has(task.targetId) && // No task with same targetId running
      (maxThreads === 0 || runningTasks.size < maxThreads) // Thread slot available
    );
  };

  // Schedule the next available task
  const scheduleNextTask = () => {
    for (let i = 0; i < pendingTasks.length; i++) {
      const task = pendingTasks[i];
      if (canStartTask(task)) {
        pendingTasks.splice(i, 1); // Remove from pending
        executeTask(task); // Start task asynchronously
        return; // Exit after starting one task
      }
    }
  };

  // Fetch tasks from the queue
  const fetchTasks = async () => {
    if (queueExhausted) return;
    const next = await iterator.next();
    if (next.done) {
      queueExhausted = true;
    } else {
      pendingTasks.push(next.value);
    }
  };

  // Main execution loop
  while (!queueExhausted || pendingTasks.length > 0 || runningTasks.size > 0) {
    // Fetch new tasks if possible
    await fetchTasks();

    // Start as many tasks as allowed
    let tasksStarted = false;
    for (let i = pendingTasks.length - 1; i >= 0; i--) {
      const task = pendingTasks[i];
      if (canStartTask(task)) {
        pendingTasks.splice(i, 1);
        executeTask(task); // Start task asynchronously
        tasksStarted = true;
      }
    }

    // If no tasks were started and some are running, wait for one to finish
    if (!tasksStarted && runningTasks.size > 0) {
      await Promise.race(runningTasks.values());
    } else if (runningTasks.size === 0 && pendingTasks.length > 0 && !queueExhausted) {
      // If nothing’s running but tasks are pending, wait briefly for queue
      await new Promise(resolve => setTimeout(resolve, 1));
    }
  }
}