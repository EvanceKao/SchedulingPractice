using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Extensions.Hosting;
using SchedulingPractice.Core;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading;

namespace SubWorker.EvanceDemo.V3
{
    public abstract class JobExecutorV2 : IDisposable
    {
        private static Random _random = new Random();

        //public bool IsStandBy { get; private set; }
        public volatile bool IsStandBy = true;
        public CancellationToken CancelDirective { get; set; }
        public event EventHandler ExecutionFinished;

        public bool NoJobs => _upcomingJobs.IsEmpty;


        public string ExecutorName
        {
            get
            {
                return _executorName;
            }

            set
            {
                //ExecutorName = value;
                _executorName = value;
            }
        }
        protected string _executorName;

        //protected JobInfo _jobInfo;
        protected ConcurrentQueue<JobInfo> _upcomingJobs = new ConcurrentQueue<JobInfo>();

        public ConcurrentQueue<JobInfo> UpcomingJobs { get; set; }

        //private CancellationToken _cancelDirective;
        private CancellationTokenSource _standByCancellationTokenSource;
        private CancellationToken _standByDirective;

        protected JobExecutorV2()
        {
            //_cancelDirective = cancelDirective;


            //Task.Run(() => gogo(), _cancelDirective);
            //Task.Factory.StartNew(() => StandByForJob(), CancelDirective, TaskCreationOptions.LongRunning, TaskScheduler.Current);

            //_cancellationTokenSource = new CancellationTokenSource();
            //_cancellationToken = _cancellationTokenSource.Token;
        }

        public virtual void Initialize()
        {
            Task.Factory.StartNew(async () => await StandByForJob(), CancelDirective, TaskCreationOptions.LongRunning, TaskScheduler.Current);
        }


        protected JobExecutorV2(CancellationToken cancelDirective)
        {
            //_cancelDirective = cancelDirective;


            //Task.Run(() => gogo(), _cancelDirective);
            CancelDirective = cancelDirective;
            //Initialize();

            //_cancellationTokenSource = new CancellationTokenSource();
            //_cancellationToken = _cancellationTokenSource.Token;
        }

        public async Task StandByForJob()
        {
            var i = 0;
            while (!CancelDirective.IsCancellationRequested)
            {
                Console.WriteLine($"ExecutorName: {_executorName}, round: {i++:00000}, {DateTime.Now}");

                this.IsStandBy = true;
                //_standByCancellationTokenSource = new CancellationTokenSource();
                //_standByDirective = _standByCancellationTokenSource.Token;

                SpinWait.SpinUntil(() => !UpcomingJobs.IsEmpty);

                this.IsStandBy = false;





                //using (var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(CancelDirective, _standByDirective))
                //{
                //    //var standByTask = Task.Run(async () =>
                //    //{
                //    //    await Task.Delay(-1, linkedCts.Token);
                //    //},
                //    //linkedCts.Token);

                //    try
                //    {
                //        //await ExecuteInterval(_cancellationTokenSource.Token);
                //        //await ExecuteInterval(linkedCts.Token);

                //        //await standByTask;
                //        await Task.Delay(-1, linkedCts.Token);
                //    }
                //    //catch (OperationCanceledException)
                //    //{
                //    //    if (_cancellationTokenSource.Token.IsCancellationRequested)
                //    //    {
                //    //        Console.WriteLine("Operation timed out.");
                //    //    }
                //    //    else if (_parentCancellationToken.IsCancellationRequested)
                //    //    {
                //    //        Console.WriteLine("Cancelling per user request.");
                //    //        //_parentCancellationToken.ThrowIfCancellationRequested();
                //    //    }
                //    //}
                //    catch (OperationCanceledException)
                //    {
                //        if (CancelDirective.IsCancellationRequested)
                //        {
                //            Console.WriteLine("上頭說別做了");
                //            return;
                //        }

                //        if (_standByDirective.IsCancellationRequested)
                //        {
                //            Console.WriteLine("有任務，解除待命");
                //            //externalToken.ThrowIfCancellationRequested();
                //        }
                //    }
                //    catch (Exception e)
                //    {
                //        Console.WriteLine($"{nameof(JobExecutor)} error.  {e}");
                //        //Console.ReadKey();
                //    }
                //    finally
                //    {
                //        _standByCancellationTokenSource.Dispose();
                //    }
                //}

                if (!UpcomingJobs.TryDequeue(out var jobInfo))
                {
                    continue;
                }


                Console.WriteLine($"ExecutorName: {_executorName}, job id: {jobInfo.Id}, round: {i}, before execute {DateTime.Now}");

                await ExecuteInternal(jobInfo);

                Console.WriteLine($"ExecutorName: {_executorName}, job id: {jobInfo.Id}, round: {i}, after execute {DateTime.Now}");




            }

        }

        public virtual void AssignJob(JobInfo jobInfo)
        {
            //_jobInfo = jobInfo;
            _upcomingJobs.Enqueue(jobInfo);

            //_standByCancellationTokenSource?.Cancel();
        }

        private async Task ExecuteInternal(JobInfo jobInfo)
        {


            try
            {

                await BeforeDelayJob(jobInfo);

                var randomWaitTime = _random.Next(0, 500);
                var timeOfWaitingForRunning = ((int)(jobInfo.RunAt - DateTime.Now).TotalMilliseconds) - randomWaitTime;
                if (timeOfWaitingForRunning > 0)
                {
                    Console.WriteLine($"ExecutorName: {_executorName}, job id: {jobInfo.Id}, RunAt: {jobInfo.RunAt}, Now: {DateTime.Now}, delay: {timeOfWaitingForRunning}");
                    await Task.Delay(timeOfWaitingForRunning, CancelDirective);
                    Console.WriteLine($"ExecutorName: {_executorName}, job id: {jobInfo.Id} wake, Now: {DateTime.Now}");
                }

                if (CancelDirective.IsCancellationRequested)
                {
                    return;
                }

                await ExecuteJob(jobInfo);

                //OnExecutionFinished(EventArgs.Empty);
            }
            catch (OperationCanceledException oce)
            {
                Console.WriteLine($"job id: {jobInfo?.Id}, cancelled.");
            }
            catch (Exception e)
            {
                Console.WriteLine($"job id: {jobInfo?.Id}, Exception: {e}");
            }
            finally
            {
                //_jobInfo = null;
                OnExecutionFinished(EventArgs.Empty);
            }
        }

        public abstract Task BeforeDelayJob(JobInfo jobInfo);
        public abstract Task ExecuteJob(JobInfo jobInfo);

        protected virtual void OnExecutionFinished(EventArgs e)
        {
            //var handler = ExecutionFinished;
            //if (handler != null)
            //{
            //    handler(this, e);
            //}

            ExecutionFinished?.Invoke(this, e);
        }

        public virtual void Dispose()
        {
            _standByCancellationTokenSource?.Dispose();
        }
    }
}
