// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Test.Microsoft.Amqp.TestCases
{
    using global::Microsoft.Azure.Amqp;
    using global::Microsoft.Azure.Amqp.Framing;
    using System;
    using System.Threading.Tasks;
    using Test.Microsoft.Azure.Amqp;
    using TestAmqpBroker;
    using Xunit;

    [Trait("Category", TestCategory.Current)]
    public class ReconnectTests : IClassFixture<TestAmqpBrokerFixture>
    {
        Uri addressUri;
        TestAmqpBroker broker;

        public ReconnectTests(TestAmqpBrokerFixture testAmqpBrokerFixture)
        {
            addressUri = TestAmqpBrokerFixture.Address;
            broker = testAmqpBrokerFixture.Broker;
        }

        [Fact]
        public async Task ConnectionRecoveryTest()
        {
            AmqpConnectionSettings connectionSettings = new AmqpConnectionSettings()
            {
                EnableLinkRecovery = true
            };

            AmqpConnection originalConnection = await AmqpConnection.Factory.OpenConnectionAsync(addressUri, connectionSettings, TimeSpan.FromSeconds(60));
            originalConnection.Settings.Properties = new Fields();
            originalConnection.Settings.Properties.Add("MyProp", "MyPropertyValue");
            AmqpConnection newConnection = null;

            try
            {
                // Test createing a new connection with the settings obtained while original connection is still active.
                connectionSettings = originalConnection.GetSettingsForRecovery();
                Assert.Null(connectionSettings.RemoteContainerId);
                Assert.Null(connectionSettings.RemoteHostName);
                newConnection = await AmqpConnection.Factory.OpenConnectionAsync(addressUri, connectionSettings, AmqpConstants.DefaultTimeout);
                Assert.Equal(originalConnection.Settings.ContainerId, newConnection.Settings.ContainerId);
                Assert.Equal(originalConnection.Settings.HostName, newConnection.Settings.HostName);
                Assert.Equal("MyPropertyValue", newConnection.Settings.Properties["MyProp"]);
                await newConnection.OpenSessionAsync(); // verify the connection is usable.
                await newConnection.CloseAsync();

                // Test createing a new connection with the settings obtained, while original connection is already closed.
                await originalConnection.CloseAsync();
                connectionSettings = originalConnection.GetSettingsForRecovery();
                Assert.Null(connectionSettings.RemoteContainerId);
                Assert.Null(connectionSettings.RemoteHostName);
                newConnection = await AmqpConnection.Factory.OpenConnectionAsync(addressUri, connectionSettings, AmqpConstants.DefaultTimeout);
                Assert.Equal(originalConnection.Settings.ContainerId, newConnection.Settings.ContainerId);
                Assert.Equal(originalConnection.Settings.HostName, newConnection.Settings.HostName);
                Assert.Equal("MyPropertyValue", newConnection.Settings.Properties["MyProp"]);
                await newConnection.OpenSessionAsync(); // verify the connection is usable.
            }
            finally
            {
                newConnection?.Close();
            }
        }

        [Fact]
        public async Task SenderRecoveryTest()
        {
            AmqpConnection connection = null;
            AmqpConnectionSettings connectionSettings = new AmqpConnectionSettings()
            {
                EnableLinkRecovery = true
            };

            try
            {
                connection = await AmqpConnection.Factory.OpenConnectionAsync(addressUri, connectionSettings, TimeSpan.FromSeconds(60));
                AmqpSession session = await connection.OpenSessionAsync();
                SendingAmqpLink originalSender = await session.OpenLinkAsync<SendingAmqpLink>("sender", "address");
                originalSender.Settings.AddProperty("MyProp", "MyPropValue");
                await originalSender.SendMessageAsync(AmqpMessage.Create("Hello World!"));
                await originalSender.CloseAsync();

                AmqpLinkTerminus linkTerminus = originalSender.GetLinkTerminus();
                SendingAmqpLink newSender = await session.RecoverLinkAsync<SendingAmqpLink>(linkTerminus);
                Assert.Equal(originalSender.Name, newSender.Name);
                Assert.Equal(originalSender.Settings.LinkName, newSender.Settings.LinkName);
                Assert.Equal(originalSender.IsReceiver, newSender.IsReceiver);
                Assert.Equal("MyPropValue", newSender.Settings.Properties["MyProp"]);

                ReceivingAmqpLink testReceiver = await session.OpenLinkAsync<ReceivingAmqpLink>("testReceiver", "address");
                await newSender.SendMessageAsync(AmqpMessage.Create("Hello World2!"));
                Assert.NotNull(await testReceiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(5000)));
            }
            finally
            {
                connection?.Close();
            }
        }

        [Fact]
        public async Task ReceiverRecoveryTest()
        {
            AmqpConnection connection = null;
            AmqpConnectionSettings connectionSettings = new AmqpConnectionSettings()
            {
                EnableLinkRecovery = true
            };

            try
            {
                connection = await AmqpConnection.Factory.OpenConnectionAsync(addressUri, connectionSettings, TimeSpan.FromSeconds(60));
                AmqpSession session = await connection.OpenSessionAsync();
                ReceivingAmqpLink originalReceiver = await session.OpenLinkAsync<ReceivingAmqpLink>("receiver", "address");
                originalReceiver.Settings.AddProperty("MyProp", "MyPropValue");
                originalReceiver.SetTotalLinkCredit(0, true); // The credit from the old link should not affect the new recovered link to be opened.
                await originalReceiver.CloseAsync();

                AmqpLinkTerminus linkTerminus = originalReceiver.GetLinkTerminus();
                ReceivingAmqpLink newReceiver = await session.RecoverLinkAsync<ReceivingAmqpLink>(linkTerminus);
                Assert.Equal(originalReceiver.Name, newReceiver.Name);
                Assert.Equal(originalReceiver.Settings.LinkName, newReceiver.Settings.LinkName);
                Assert.Equal(originalReceiver.IsReceiver, newReceiver.IsReceiver);
                Assert.Equal("MyPropValue", newReceiver.Settings.Properties["MyProp"]);

                SendingAmqpLink testSender = await session.OpenLinkAsync<SendingAmqpLink>("testSender", "address");
                await testSender.SendMessageAsync(AmqpMessage.Create("Hello World2!"));
                Assert.NotNull(await newReceiver.ReceiveMessageAsync(TimeSpan.FromMilliseconds(5000)));
            }
            finally
            {
                connection?.Close();
            }
        }

        [Fact]
        public async Task LinkRecoveryNameUniquenessTest()
        {
            string linkName = "MyLink";
            AmqpConnection nonRecoverableConnection = await AmqpConnection.Factory.OpenConnectionAsync(addressUri);
            AmqpConnection recoverableConnection = await AmqpConnection.Factory.OpenConnectionAsync(addressUri, new AmqpConnectionSettings() {EnableLinkRecovery = true}, TimeSpan.FromSeconds(60));

            try
            {
                AmqpSession nonRecoverableSession1 = await nonRecoverableConnection.OpenSessionAsync(new AmqpSessionSettings());
                AmqpSession nonRecoverableSession2 = await nonRecoverableConnection.OpenSessionAsync(new AmqpSessionSettings());
                AmqpSession recoverableSession1 = await recoverableConnection.OpenSessionAsync(new AmqpSessionSettings());
                AmqpSession recoverableSession2 = await recoverableConnection.OpenSessionAsync(new AmqpSessionSettings());

                // Having duplicate link names on smae connection is allowed without link recovery.
                SendingAmqpLink sendingLink1 = await nonRecoverableSession1.OpenLinkAsync<SendingAmqpLink>(linkName, addressUri.AbsoluteUri);
                SendingAmqpLink sendingLink2 = await nonRecoverableSession2.OpenLinkAsync<SendingAmqpLink>(linkName, addressUri.AbsoluteUri);
                await sendingLink1.CloseAsync();
                await sendingLink2.CloseAsync();

                ReceivingAmqpLink receivingLink1 = await nonRecoverableSession1.OpenLinkAsync<ReceivingAmqpLink>(linkName, addressUri.AbsoluteUri);
                ReceivingAmqpLink receivingLink2 = await nonRecoverableSession2.OpenLinkAsync<ReceivingAmqpLink>(linkName, addressUri.AbsoluteUri);
                await receivingLink1.CloseAsync();
                await receivingLink2.CloseAsync();

                // Having duplicate link names on same connection is not allowed if link recovery is enabled.
                sendingLink1 = await recoverableSession1.OpenLinkAsync<SendingAmqpLink>(linkName, addressUri.AbsoluteUri);
                await Assert.ThrowsAsync<InvalidOperationException>(() => recoverableSession2.OpenLinkAsync<SendingAmqpLink>(linkName, addressUri.AbsoluteUri));
                await sendingLink1.CloseAsync();

                receivingLink1 = await recoverableSession1.OpenLinkAsync<ReceivingAmqpLink>(linkName, addressUri.AbsoluteUri);
                await Assert.ThrowsAsync<InvalidOperationException>(() => recoverableSession2.OpenLinkAsync<ReceivingAmqpLink>(linkName, addressUri.AbsoluteUri));
                await receivingLink1.CloseAsync();

                // Having duplicate link names on same connection is allowed if roles are different.
                sendingLink1 = await recoverableSession1.OpenLinkAsync<SendingAmqpLink>(linkName, addressUri.AbsoluteUri);
                receivingLink1 = await recoverableSession2.OpenLinkAsync<ReceivingAmqpLink>(linkName, addressUri.AbsoluteUri);
                await sendingLink1.CloseAsync();
                await receivingLink1.CloseAsync();

                // Check duplicate link termini on recovery as well.
                sendingLink1 = await recoverableSession1.OpenLinkAsync<SendingAmqpLink>(linkName, addressUri.AbsoluteUri);
                AmqpLinkTerminus terminus = sendingLink1.GetLinkTerminus();
                await Assert.ThrowsAsync<InvalidOperationException>(() => recoverableSession2.RecoverLinkAsync<SendingAmqpLink>(terminus));
                await sendingLink1.CloseAsync();

                receivingLink1 = await recoverableSession1.OpenLinkAsync<ReceivingAmqpLink>(linkName, addressUri.AbsoluteUri);
                terminus = receivingLink1.GetLinkTerminus();
                await Assert.ThrowsAsync<InvalidOperationException>(() => recoverableSession2.RecoverLinkAsync<ReceivingAmqpLink>(terminus));
                await receivingLink1.CloseAsync();

                // Ensure that the terminus is removed once it closes.
                sendingLink2 = await recoverableSession2.OpenLinkAsync<SendingAmqpLink>(linkName, addressUri.AbsoluteUri);
                terminus = sendingLink1.GetLinkTerminus();
                await sendingLink2.CloseAsync();
                sendingLink1 = await recoverableSession1.RecoverLinkAsync<SendingAmqpLink>(terminus);
                sendingLink1.Abort();
                sendingLink2 = await recoverableSession2.RecoverLinkAsync<SendingAmqpLink>(terminus);
                await sendingLink2.CloseAsync();
            }
            finally
            {
                await nonRecoverableConnection.CloseAsync();
                await recoverableConnection.CloseAsync();
            }
        }

        async Task RestartBrokerAsync()
        {
            // Restart the broker. All connections should be disconnected from the broker side.
            broker.Stop();
            await Task.Delay(1000);
            broker.Start();
        }
    }
}