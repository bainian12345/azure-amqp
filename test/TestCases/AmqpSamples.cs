// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Test.Microsoft.Azure.Amqp
{
    using System;
    using System.Threading.Tasks;
    using global::Microsoft.Azure.Amqp;
    using TestAmqpBroker;
    using Xunit;

    [Collection("Sequential")]
    [Trait("Category", TestCategory.Current)]
    public class AmqpSamples : IClassFixture<TestAmqpBrokerFixture>
    {
        Uri addressUri;
        TestAmqpBroker broker;

        public AmqpSamples(TestAmqpBrokerFixture testAmqpBrokerFixture)
        {
            addressUri = TestAmqpBrokerFixture.Address;
            broker = testAmqpBrokerFixture.Broker;
        }

        [Fact]
        public async Task SendReceiveSample()
        {
            string queue = "SendReceiveSample";
            broker.AddQueue(queue);

            var factory = new AmqpConnectionFactory();
            var connection = await factory.OpenConnectionAsync(addressUri);
            var session = await connection.OpenSessionAsync();

            var sender = await session.OpenLinkAsync<SendingAmqpLink>("sender", queue);
            var outcome = await sender.SendMessageAsync(AmqpMessage.Create("Hello World!"));
            await sender.CloseAsync();

            var receiver = await session.OpenLinkAsync<ReceivingAmqpLink>("receiver", queue);
            var message = await receiver.ReceiveMessageAsync();
            string body = (string)message.ValueBody.Value;
            receiver.AcceptMessage(message);
            await receiver.CloseAsync();

            await connection.CloseAsync();
        }

        [Fact]
        public async Task LinkRecoverySample()
        {
            string queueName = "LinkRecoverySample";
            broker.AddQueue(queueName);

            // Need to provide a AmqpLinkTerminusManager instance to ensure the uniqueness of the link endpoints and specify the desired expiration policy.
            var terminusManager = new AmqpLinkTerminusManager() { ExpirationPolicy = LinkTerminusExpirationPolicy.NEVER };
            var amqpSettings = new AmqpSettings() { RuntimeProvider = new TestRuntimeProvider(terminusManager) };
            var factory = new AmqpConnectionFactory(amqpSettings);

            // Need to use the same containId later to identify and recover this link endpoint.
            string containerId = Guid.NewGuid().ToString();
            var connection = await factory.OpenConnectionAsync(addressUri, new AmqpConnectionSettings() { ContainerId = containerId }, TimeSpan.FromMinutes(1));

            try
            {
                // Send and receive the message normally.
                var session = await connection.OpenSessionAsync();
                var sender = await session.OpenLinkAsync<SendingAmqpLink>("sender", queueName);
                await sender.SendMessageAsync(AmqpMessage.Create("Hello World!"));
                var receiver = await session.OpenLinkAsync<ReceivingAmqpLink>("receiver", queueName);
                var message = await receiver.ReceiveMessageAsync();

                // Restart the broker. All connections should be disconnected from the broker side.
                broker.Stop();
                await Task.Delay(1000);
                broker.Start();

                // Try to complete the received message now. Should throw exception because the link is closed.
                Assert.Throws<AmqpException>(() => receiver.AcceptMessage(message));

                // Need to reconnect with the same containerId and link terminus for link recovery.
                AmqpConnectionSettings connectionRecoverySettings = new AmqpConnectionSettings() { ContainerId = connection.Settings.ContainerId };
                connection = await factory.OpenConnectionAsync(addressUri, connectionRecoverySettings, AmqpConstants.DefaultTimeout);
                session = await connection.OpenSessionAsync();
                receiver = await session.RecoverLinkAsync<ReceivingAmqpLink>(receiver.Terminus, queueName);
                receiver.AcceptMessage(message);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
            finally
            {
                await connection.CloseAsync();
            }
        }
    }
}
