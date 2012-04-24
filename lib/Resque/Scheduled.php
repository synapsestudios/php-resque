<?php
/**
 * Resque worker that handles checking queues for jobs, fetching them
 * off the queues, running them and handling the result.
 *
 */
class Resque_Scheduled
{
	const LOG_NONE = 0;
	const LOG_NORMAL = 1;
	const LOG_VERBOSE = 2;

	/**
	 * @var int Current log level of this worker.
	 */
	public $logLevel = 0;

	/**
	 * @var string The hostname of this worker.
	 */
	private $hostname;

	/**
	 * @var boolean True if on the next iteration, the worker should shutdown.
	 */
	private $shutdown = false;

	/**
	 * @var boolean True if this worker is paused.
	 */
	private $paused = false;

	/**
	 * Instantiate a scheduled task runner. To avoid having duplicate items
	 * added to the queue only one instance should be run.
	 *
	 */
	public function __construct()
	{
		if (function_exists('gethostname'))
		{
			$hostname = gethostname();
		}
		else
		{
			$hostname = php_uname('n');
		}

		$this->hostname = $hostname;
		$this->id = $this->hostname . ':'.getmypid() . ': Scheduled Tasks';
	}

	/**
	 * The primary loop for a worker which when called on an instance starts
	 * the worker's life cycle.
	 *
	 * Queues are checked every $interval (seconds) for new jobs.
	 *
	 * @param int $interval How often to check for new jobs across the queues.
	 */
	public function work($interval = 5)
	{
		$this->startup();

		while(true)
		{

			if($this->shutdown)
				break;

			if ( ! $this->paused)
			{
				$this->log('Looking for jobs');

				$this->proccessDelayedJobs();

				$this->log('Waiting for jobs');
				usleep($interval * 1000000);
			}
		}
	}

	/**
	 * Process delayed jobs. This method will add delayed jobs to their 
	 * queue once the delayed amount of time has passed.
	 * 
	 * @return void
	 */
	protected function proccessDelayedJobs()
	{
		do
		{
			$timestamp = Resque::next_delayed_timestamp();

			do
			{
				$job = Resque::next_item_for_timestamp($timestamp);

				if ($job)
				{
					$args = isset($job['args']) ? $job['args'] : array();

					// Add the job to the queue
					Resque::enqueue($job['queue'], $job['class'], $args);

					$this->log('Adding delayed job to queue');
				}
			}
			while($job);
		}
		while ($timestamp);
	}

	/**
	 * Perform necessary actions to start a worker.
	 */
	private function startup()
	{
		$this->registerSigHandlers();
	}

	/**
	 * Register signal handlers that a worker should respond to.
	 *
	 * TERM: Shutdown immediately and stop processing jobs.
	 * INT: Shutdown immediately and stop processing jobs.
	 * QUIT: Shutdown after the current job finishes processing.
	 * USR1: Kill the forked child immediately and continue processing jobs.
	 */
	private function registerSigHandlers()
	{
		if(!function_exists('pcntl_signal')) {
			return;
		}

		declare(ticks = 1);
		pcntl_signal(SIGTERM, array($this, 'shutDownNow'));
		pcntl_signal(SIGINT, array($this, 'shutDownNow'));
		pcntl_signal(SIGQUIT, array($this, 'shutdown'));
		pcntl_signal(SIGUSR1, array($this, 'shutdown'));
		pcntl_signal(SIGUSR2, array($this, 'pauseProcessing'));
		pcntl_signal(SIGCONT, array($this, 'unPauseProcessing'));
		pcntl_signal(SIGPIPE, array($this, 'reestablishRedisConnection'));
		$this->log('Registered signals', self::LOG_VERBOSE);
	}

	/**
	 * Signal handler callback for USR2, pauses processing of new jobs.
	 */
	public function pauseProcessing()
	{
		$this->log('USR2 received; pausing job processing');
		$this->paused = true;
	}

	/**
	 * Signal handler callback for CONT, resumes worker allowing it to pick
	 * up new jobs.
	 */
	public function unPauseProcessing()
	{
		$this->log('CONT received; resuming job processing');
		$this->paused = false;
	}

	/**
	 * Signal handler for SIGPIPE, in the event the redis connection has gone away.
	 * Attempts to reconnect to redis, or raises an Exception.
	 */
	public function reestablishRedisConnection()
	{
		$this->log('SIGPIPE received; attempting to reconnect');
		Resque::redis()->establishConnection();
	}

	/**
	 * Schedule a worker for shutdown. Will finish processing the current job
	 * and when the timeout interval is reached, the worker will shut down.
	 */
	public function shutdown()
	{
		$this->shutdown = true;
		$this->log('Exiting...');
	}

	/**
	 * Force an immediate shutdown of the worker, killing any child jobs
	 * currently running.
	 */
	public function shutdownNow()
	{
		$this->shutdown();
	}

	public function log($message)
	{
		if($this->logLevel == self::LOG_NORMAL) {
			fwrite(STDOUT, "*** " . $message . "\n");
		}
		else if($this->logLevel == self::LOG_VERBOSE) {
			fwrite(STDOUT, "** [" . strftime('%T %Y-%m-%d') . "] " . $message . "\n");
		}
	}
}