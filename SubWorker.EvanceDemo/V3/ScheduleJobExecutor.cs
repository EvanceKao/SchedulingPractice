using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
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

namespace SubWorker.EvanceDemo.V3
{
    public class ScheduleJobExecutor : JobExecutorV2
    {
        private JobsRepo _jobsRepo = new JobsRepo();

        public ScheduleJobExecutor() : base()
        {
        }

        public ScheduleJobExecutor(CancellationToken cancelDirective) : base(cancelDirective)
        {
        }

        public override async Task BeforeDelayJob(JobInfo jobInfo)
        {
            //throw new NotImplementedException();
            return;
        }

        public override async Task ExecuteJob(JobInfo jobInfo)
        {
            if (_jobsRepo.GetJob(jobInfo.Id).State != 0)
            {
                return;
            }

            // lock job 後就不傳 cancellation token
            if (_jobsRepo.AcquireJobLock(jobInfo.Id))
            {
                //await _semaphoreSlim.WaitAsync(cancellationToken);
                //await _semaphoreSlim.WaitAsync();

                //if (jobInfo.RunAt > DateTime.Now)
                //{
                //    await Task.Delay((int)(jobInfo.RunAt - DateTime.Now).TotalMilliseconds);
                //}

                SpinWait.SpinUntil(() => DateTime.Now >= jobInfo.RunAt);

                _jobsRepo.ProcessLockedJob(jobInfo.Id);

                //_semaphoreSlim.Release();

                Console.WriteLine($"ExecutorName: {_executorName}, job id: {jobInfo.Id}, O");
            }
            else
            {
                Console.WriteLine($"ExecutorName: {_executorName}, job id: {jobInfo.Id}, X");

                //Console.Write("X");
            }
        }

        public override void Dispose()
        {
            base.Dispose();
            //_jobsRepo?.Dispose();
        }
    }
}
