using Microsoft.Extensions.Hosting;
using SchedulingPractice.Core;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SubWorker.EvanceDemo
{
    public class EvanceSubWorkerBackgroundServiceV2 : BackgroundService
    {
        private static readonly SemaphoreSlim _semaphoreSlim = new SemaphoreSlim(5, 5);


        private static readonly TimeSpan _getJobsDuration = JobSettings.MinPrepareTime;
        private static readonly int _maxDegreeOfParallelism = 5;
        private static ParallelOptions _parallelOptions;
        ///// <summary>
        ///// key: job ID
        ///// value: RunAt ticks
        ///// </summary>
        //private static ConcurrentDictionary<int, long> _upcomingJobs = new ConcurrentDictionary<int, long>();
        /// <summary>
        /// key: job ID
        /// value: JobExecutor
        /// </summary>
        private static Dictionary<int, JobExecutor> _upcomingJobs = new Dictionary<int, JobExecutor>();

        protected async override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var random = new Random();
            await Task.Delay(random.Next(0, 1000), stoppingToken);
            //await Task.Delay(1);

            using (JobsRepo jobRepo = new JobsRepo())
            {
                while (stoppingToken.IsCancellationRequested == false)
                {
                    try
                    {
                        Console.WriteLine($"Get ready jobs... {DateTime.Now}");

                        var readyJobs = jobRepo.GetReadyJobs(_getJobsDuration);

                        foreach (var jobInfo in readyJobs)
                        {
                            if (_upcomingJobs.ContainsKey(jobInfo.Id))
                            {
                                continue;
                            }

                            var jobExecutor = new JobExecutor(jobInfo, _semaphoreSlim, stoppingToken);
                            // TODO: fix this
                            //Task.Run(() => jobExecutor.Execute());
                            jobExecutor.Execute();
                            _upcomingJobs.Add(jobInfo.Id, jobExecutor);
                        }

                        var exceptResult = _upcomingJobs.Select(kv => kv.Key).Except(readyJobs.Select(x => x.Id)).ToHashSet();
                        foreach (var jobId in exceptResult)
                        {
                            _upcomingJobs[jobId].StopExecutor();
                            _upcomingJobs.Remove(jobId);
                        }



                        Console.WriteLine($"Get ready jobs end... {DateTime.Now}");

                        try
                        {
                            await Task.Delay(JobSettings.MinPrepareTime, stoppingToken);
                            Console.Write("_");
                        }
                        catch (TaskCanceledException) { break; }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex);
                        Console.ReadKey();
                    }
                }
            }

            Console.WriteLine($"- shutdown event detected, stop worker service...");

        }

    }
}
