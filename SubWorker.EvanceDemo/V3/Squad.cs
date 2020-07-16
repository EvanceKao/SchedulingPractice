using System;
using System.Collections.Generic;
using System.Text;
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
    public class Squad<T> where T : JobExecutorV2, new()
    {
        private readonly object _squadLock = new object();

        //private ConcurrentBag<T> _squad;
        private List<T> _squad;
        private SemaphoreSlim _availableExecutor;

        private CancellationToken _cancelDirective;

        public int ExecutorCount { get; private set; }


        private ConcurrentQueue<JobInfo> _upcomingJobs = new ConcurrentQueue<JobInfo>();



        public Squad(int executorCount, CancellationToken cancelDirective)
        {
            if (executorCount < 1)
            {
                throw new ArgumentOutOfRangeException(nameof(executorCount), "Must bigger than 0");
            }

            ExecutorCount = executorCount;
            _cancelDirective = cancelDirective;
            //_squad = new ConcurrentBag<T>();
            _squad = new List<T>(ExecutorCount);
            _availableExecutor = new SemaphoreSlim(ExecutorCount, ExecutorCount);

            CreateSquad();
        }

        private void CreateSquad()
        {
            for (int i = 0; i < ExecutorCount; i++)
            {
                var jobExecutor = new T
                {
                    ExecutorName = i.ToString("00"),
                    CancelDirective = _cancelDirective,
                    UpcomingJobs = _upcomingJobs
                };

                jobExecutor.ExecutionFinished += ExecutionFinished;
                jobExecutor.Initialize();
                _squad.Add(jobExecutor);
            }
        }

        private void ExecutionFinished(object sender, EventArgs e)
        {
            //_availableExecutor.Release();
        }

        public async void AssignJob(JobInfo jobInfo)
        {
            //await _availableExecutor.WaitAsync();
            //lock (_squadLock)
            //{
            //var executor = _squad.FirstOrDefault(s => s.IsStandBy);

            //var eIndex = jobInfo.Id % ExecutorCount;
            //var executor = _squad[eIndex];

            //_availableExecutor.Wait();
            //lock (_squadLock)
            //{
            //var executor = _squad.First(s => s.NoJobs);

            _upcomingJobs.Enqueue(jobInfo);

            //Console.WriteLine($"Assign JobId: {jobInfo?.Id}, executorName: {executor.ExecutorName}");
            //executor?.AssignJob(jobInfo);
            //}
        }

    }
}
