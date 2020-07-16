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

namespace SubWorker.EvanceDemo
{
    public class JobExecutor : IDisposable
    {
        private static Random _random = new Random();

        private JobInfo _jobInfo;
        private SemaphoreSlim _semaphoreSlim;
        private CancellationToken _parentCancellationToken;
        private CancellationTokenSource _cancellationTokenSource;
        private CancellationToken _cancellationToken;

        public JobExecutor(JobInfo jobInfo, SemaphoreSlim semaphoreSlim, CancellationToken parentCancellationToken)
        {
            _jobInfo = jobInfo ?? throw new ArgumentNullException(nameof(jobInfo));
            _semaphoreSlim = semaphoreSlim ?? throw new ArgumentNullException(nameof(semaphoreSlim));
            _parentCancellationToken = parentCancellationToken;
            _cancellationTokenSource = new CancellationTokenSource();
            _cancellationToken = _cancellationTokenSource.Token;
        }

        public async void Execute()
        {
            using (var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_parentCancellationToken, _cancellationToken))
            {
                var task = Task.Run(() => ExecuteInterval(linkedCts.Token), linkedCts.Token);

                try
                {
                    //await ExecuteInterval(_cancellationTokenSource.Token);
                    //await ExecuteInterval(linkedCts.Token);
                    await task;
                }
                //catch (OperationCanceledException)
                //{
                //    if (_cancellationTokenSource.Token.IsCancellationRequested)
                //    {
                //        Console.WriteLine("Operation timed out.");
                //    }
                //    else if (_parentCancellationToken.IsCancellationRequested)
                //    {
                //        Console.WriteLine("Cancelling per user request.");
                //        //_parentCancellationToken.ThrowIfCancellationRequested();
                //    }
                //}
                catch (Exception e)
                {
                    Console.WriteLine($"{nameof(JobExecutor)} error.  {e}");
                    //Console.ReadKey();
                }
                finally
                {
                    _cancellationTokenSource?.Dispose();
                }

            }
        }


        //private Task ExecuteInterval(CancellationToken cancellationToken)
        //{
        //    var task = Task.Run(async () =>
        //    {
        //        using (var jobRepo = new JobsRepo())
        //        {
        //            var timeOfWaitingForRunning = (_jobInfo.RunAt - DateTime.Now).TotalMilliseconds;
        //            if (timeOfWaitingForRunning > 0)
        //            {
        //                Console.WriteLine($"job id: {_jobInfo.Id}, RunAt: {_jobInfo.RunAt}, Now: {DateTime.Now}, delay: {timeOfWaitingForRunning}");
        //                await Task.Delay((int)timeOfWaitingForRunning, cancellationToken);
        //                Console.WriteLine($"job id: {_jobInfo.Id} wake, Now: {DateTime.Now}");
        //            }

        //            if (cancellationToken.IsCancellationRequested)
        //            {
        //                cancellationToken.ThrowIfCancellationRequested();
        //                return;
        //            }

        //            if (jobRepo.GetJob(_jobInfo.Id).State != 0)
        //            {
        //                return;
        //            }

        //            if (jobRepo.AcquireJobLock(_jobInfo.Id))
        //            {
        //                //await _semaphoreSlim.WaitAsync(cancellationToken);
        //                await _semaphoreSlim.WaitAsync();
        //                jobRepo.ProcessLockedJob(_jobInfo.Id);
        //                _semaphoreSlim.Release();
        //                Console.Write("O");
        //            }
        //            else
        //            {
        //                Console.Write("X");
        //            }
        //        }
        //    },
        //    cancellationToken);

        //    return task;

        //    //try
        //    //{
        //    //    await task;
        //    //    Console.WriteLine("Retrieved information for {0} files.", files.Count);
        //    //}
        //    //catch (AggregateException e)
        //    //{
        //    //    Console.WriteLine("Exception messages:");
        //    //    foreach (var ie in e.InnerExceptions)
        //    //        Console.WriteLine("   {0}: {1}", ie.GetType().Name, ie.Message);

        //    //    Console.WriteLine("\nTask status: {0}", t.Status);
        //    //}
        //    //finally
        //    //{
        //    //    tokenSource.Dispose();
        //    //}

        //    //try
        //    //{
        //    //    await task;
        //    //}
        //    //catch (OperationCanceledException)
        //    //{
        //    //    if (_cancellationToken.IsCancellationRequested)
        //    //    {
        //    //        Console.WriteLine("Operation timed out.");
        //    //    }
        //    //    else if (_parentCancellationToken.IsCancellationRequested)
        //    //    {
        //    //        Console.WriteLine("Cancelling per user request.");
        //    //        _parentCancellationToken.ThrowIfCancellationRequested();
        //    //    }
        //    //}
        //    //finally
        //    //{
        //    //    _cancellationTokenSource?.Dispose();
        //    //}
        //}

        private async void ExecuteInterval(CancellationToken cancellationToken)
        {
            using (var jobRepo = new JobsRepo())
            {
                var randomWaitTime = _random.Next(500, 1500);
                var timeOfWaitingForRunning = ((int)(_jobInfo.RunAt - DateTime.Now).TotalMilliseconds) - randomWaitTime;
                if (timeOfWaitingForRunning > 0)
                {
                    //Console.WriteLine($"job id: {_jobInfo.Id}, RunAt: {_jobInfo.RunAt}, Now: {DateTime.Now}, delay: {timeOfWaitingForRunning}");
                    await Task.Delay(timeOfWaitingForRunning, cancellationToken);
                    //Console.WriteLine($"job id: {_jobInfo.Id} wake, Now: {DateTime.Now}");
                }

                if (cancellationToken.IsCancellationRequested)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    return;
                }

                if (jobRepo.GetJob(_jobInfo.Id).State != 0)
                {
                    return;
                }

                // lock job 後就不傳 cancellation token
                if (jobRepo.AcquireJobLock(_jobInfo.Id))
                {
                    //await _semaphoreSlim.WaitAsync(cancellationToken);
                    //await _semaphoreSlim.WaitAsync();

                    if (_jobInfo.RunAt > DateTime.Now)
                    {
                        await Task.Delay(_jobInfo.RunAt - DateTime.Now);
                    }

                    jobRepo.ProcessLockedJob(_jobInfo.Id);

                    //_semaphoreSlim.Release();

                    //Console.Write("O");
                }
                else
                {
                    //Console.Write("X");
                }
            }
        }

        public void StopExecutor()
        {
            // https://github.com/SignalR/SignalR/blob/master/src/Microsoft.AspNet.SignalR.Core/Infrastructure/SafeCancellationTokenSource.cs

            try
            {
                _cancellationTokenSource?.Cancel();
            }
            catch (Exception)
            {
            }
            finally
            {
                _cancellationTokenSource?.Dispose();
            }
        }

        public void Dispose()
        {
            _cancellationTokenSource?.Dispose();
        }
    }
}
