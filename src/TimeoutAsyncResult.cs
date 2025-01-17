// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Azure.Amqp
{
    using System;
    using System.Threading;

    abstract class TimeoutAsyncResult<T> : AsyncResult where T : class
    {
        readonly TimeSpan timeout;
        readonly CancellationToken cancellationToken;
        CancellationTokenRegistration cancellationTokenRegistration;
        Timer timer;

        protected TimeoutAsyncResult(TimeSpan timeout, CancellationToken cancellationToken, AsyncCallback callback, object state)
            : base(callback, state)
        {
            // The derived class must call StartTracking to start the timer and cancelation token registration.
            // Timer is not started here because it could fire before the derived class ctor completes.
            this.timeout = timeout;
            this.cancellationToken = cancellationToken;
        }

        protected abstract T Target { get; }

        /// <summary>
        /// Called when the cancellation token is cancelled.
        /// </summary>
        public abstract void Cancel(bool isSynchronous);

        protected void StartTracking()
        {
            if (!this.IsCompleted)
            {
                if (this.timeout != Timeout.InfiniteTimeSpan && this.timeout != TimeSpan.MaxValue)
                {
                    this.timer = new Timer(static s => OnTimerCallback(s), this, this.timeout, Timeout.InfiniteTimeSpan);
                }

                if (this.cancellationToken.CanBeCanceled)
                {
                    this.cancellationTokenRegistration = this.cancellationToken.Register(
                        static o =>
                        {
                            var thisPtr = (TimeoutAsyncResult<T>)o;
                            bool isSynchronous = thisPtr.cancellationTokenRegistration == default;
                            thisPtr.Cancel(isSynchronous);
                        },
                        this);
                }
            }
        }

        protected virtual void CompleteOnTimer()
        {
            if (this.timer != null)
            {
                this.timer.Dispose();
            }

            this.CompleteInternal(false, new TimeoutException(AmqpResources.GetString(AmqpResources.AmqpTimeout, this.timeout, this.Target)));
        }

        protected bool CompleteSelf(bool syncComplete)
        {
            return this.CompleteSelf(syncComplete, null);
        }

        protected bool CompleteSelf(bool syncComplete, Exception exception)
        {
            if (this.timer != null)
            {
                this.timer.Dispose();
            }

            return this.CompleteInternal(syncComplete, exception);
        }

        static void OnTimerCallback(object state)
        {
            TimeoutAsyncResult<T> thisPtr = (TimeoutAsyncResult<T>)state;
            try
            {
                thisPtr.CompleteOnTimer();
            }
            catch (Exception ex) when (!Fx.IsFatal(ex))
            {
                AmqpTrace.Provider.AmqpLogError(thisPtr.Target, "OnTimerCallback", ex);
            }
        }

        bool CompleteInternal(bool syncComplete, Exception exception)
        {
            this.cancellationTokenRegistration.Dispose();
            return this.TryComplete(syncComplete, exception);
        }
    }
}
