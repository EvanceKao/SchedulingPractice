using Microsoft.Extensions.Hosting;
using SchedulingPractice.Core;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SubWorker.EvanceDemo
{
    public class EvanceSubWorkerBackgroundService : BackgroundService
    {
        private static readonly TimeSpan _getJobsDuration = new TimeSpan(0, 0, 30);
        private static readonly int _maxDegreeOfParallelism = 5;
        private static ParallelOptions _parallelOptions;
        /// <summary>
        /// key: job ID
        /// value: RunAt ticks
        /// </summary>
        private static ConcurrentDictionary<int, long> _upcomingJobs = new ConcurrentDictionary<int, long>();

        protected async override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await Task.Delay(1);

            _parallelOptions = new ParallelOptions
            {
                MaxDegreeOfParallelism = _maxDegreeOfParallelism,
                CancellationToken = stoppingToken
            };

            await Task.Factory.StartNew(() => DoJobs(stoppingToken), stoppingToken);

            using (JobsRepo jobRepo = new JobsRepo())
            {
                while (stoppingToken.IsCancellationRequested == false)
                {
                    try
                    {
                        Console.WriteLine($"Get ready jobs... {DateTime.Now}");

                        var readyJobs = jobRepo.GetReadyJobs(_getJobsDuration);

                        var exceptResult = _upcomingJobs.Select(kv => kv.Key).Except(readyJobs.Select(x => x.Id)).ToHashSet();
                        foreach (var jobId in exceptResult)
                        {
                            _upcomingJobs.TryRemove(jobId, out var runAtTicks);
                        }

                        foreach (var jobInfo in readyJobs)
                        {
                            _upcomingJobs.TryAdd(jobInfo.Id, jobInfo.RunAt.Ticks);
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
                    }
                }
            }

            Console.WriteLine($"- shutdown event detected, stop worker service...");
        }

        private async void DoJobs(CancellationToken stoppingToken)
        {
            while (stoppingToken.IsCancellationRequested == false)
            {
                try
                {
                    Console.WriteLine($"Do jobs... {DateTime.Now}");

                    long currentTicks = DateTime.Now.Ticks;

                    var upcomingJobIds = _upcomingJobs.Where(kv => currentTicks >= kv.Value).OrderBy(kv => kv.Value).Select(kv => kv.Key).ToList();
                    Parallel.ForEach(upcomingJobIds, _parallelOptions, jobId =>
                    {
                        _upcomingJobs.TryRemove(jobId, out var runAtTicks);

                        using (var jobRepo = new JobsRepo())
                        {
                            if (jobRepo.GetJob(jobId).State != 0)
                            {
                                return;
                            }

                            if (jobRepo.AcquireJobLock(jobId))
                            {
                                jobRepo.ProcessLockedJob(jobId);
                                Console.Write("O");
                            }
                            else
                            {
                                Console.Write("X");
                            }
                        }

                        _parallelOptions.CancellationToken.ThrowIfCancellationRequested();
                    });
                }
                catch (OperationCanceledException e)
                {
                    Console.WriteLine("OperationCanceledException: " + e.Message);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Exception: " + e.Message);
                }

                try
                {
                    await Task.Delay(1, stoppingToken);
                }
                catch (TaskCanceledException) { break; }
            }
        }
    }
}
