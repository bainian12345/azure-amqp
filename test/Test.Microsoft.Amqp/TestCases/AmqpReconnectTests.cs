// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Test.Microsoft.Amqp.TestCases
{
    using global::Microsoft.Azure.Amqp;
    using global::Microsoft.Azure.Amqp.Encoding;
    using global::Microsoft.Azure.Amqp.Framing;
    using global::Microsoft.Azure.Amqp.Transport;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Test.Microsoft.Azure.Amqp;
    using TestAmqpBroker;
    using Xunit;
    using static TestAmqpBroker.TestAmqpBroker;

    [Trait("Category", TestCategory.Current)]
    public class ReconnectTests : IClassFixture<TestAmqpBrokerFixture>
    {
        static Uri addressUri;
        static TestAmqpBroker broker;

        public ReconnectTests(TestAmqpBrokerFixture testAmqpBrokerFixture)
        {
            addressUri = TestAmqpBrokerFixture.Address;
            broker = testAmqpBrokerFixture.Broker;
        }

        // Test that the connection settings obtained from AmqpConnection.GetSettingsForRecovery() can be used to open new connections with identical settings.
        [Fact]
        public async Task ConnectionRecoveryTest()
        {
            AmqpConnectionSettings connectionSettings = new AmqpConnectionSettings()
            {
                ContainerId = nameof(ConnectionRecoveryTest),
            };

            AmqpConnection originalConnection = await AmqpConnection.Factory.OpenConnectionAsync(addressUri, connectionSettings, TimeSpan.FromSeconds(60));
            originalConnection.Settings.Properties = new Fields();
            originalConnection.Settings.Properties.Add("MyProp", "MyPropValue");
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
                Assert.Equal("MyPropValue", newConnection.Settings.Properties["MyProp"]);
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
                Assert.Equal("MyPropValue", newConnection.Settings.Properties["MyProp"]);
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
            try
            {
                connection = await OpenTestConnectionAsync(addressUri);
                AmqpSession session = await connection.OpenSessionAsync();
                SendingAmqpLink originalSender = await session.OpenLinkAsync<SendingAmqpLink>(nameof(SenderRecoveryTest) + "-sender", nameof(SenderRecoveryTest));
                originalSender.Settings.AddProperty("MyProp", "MyPropValue");
                await originalSender.SendMessageAsync(AmqpMessage.Create("Hello World!"));
                await originalSender.CloseAsync();

                AmqpLinkTerminus linkTerminus = originalSender.GetLinkTerminus();
                SendingAmqpLink newSender = await session.RecoverLinkAsync<SendingAmqpLink>(linkTerminus);
                Assert.Equal(originalSender.Name, newSender.Name);
                Assert.Equal(originalSender.Settings.LinkName, newSender.Settings.LinkName);
                Assert.Equal(originalSender.IsReceiver, newSender.IsReceiver);
                Assert.Equal("MyPropValue", newSender.Settings.Properties["MyProp"]);

                ReceivingAmqpLink testReceiver = await session.OpenLinkAsync<ReceivingAmqpLink>(nameof(SenderRecoveryTest) + "-receiver", nameof(SenderRecoveryTest));
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
            try
            {
                connection = await OpenTestConnectionAsync(addressUri);
                AmqpSession session = await connection.OpenSessionAsync();
                ReceivingAmqpLink originalReceiver = await session.OpenLinkAsync<ReceivingAmqpLink>(nameof(ReceiverRecoveryTest) + "-receiver", nameof(SenderRecoveryTest));
                originalReceiver.Settings.AddProperty("MyProp", "MyPropValue");
                originalReceiver.SetTotalLinkCredit(0, true); // The credit from the old link should not affect the new recovered link to be opened.
                await originalReceiver.CloseAsync();

                AmqpLinkTerminus linkTerminus = originalReceiver.GetLinkTerminus();
                ReceivingAmqpLink newReceiver = await session.RecoverLinkAsync<ReceivingAmqpLink>(linkTerminus);
                Assert.Equal(originalReceiver.Name, newReceiver.Name);
                Assert.Equal(originalReceiver.Settings.LinkName, newReceiver.Settings.LinkName);
                Assert.Equal(originalReceiver.IsReceiver, newReceiver.IsReceiver);
                Assert.Equal("MyPropValue", newReceiver.Settings.Properties["MyProp"]);

                SendingAmqpLink testSender = await session.OpenLinkAsync<SendingAmqpLink>(nameof(ReceiverRecoveryTest) + "-sender", nameof(SenderRecoveryTest));
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
            string linkName = nameof(LinkRecoveryNameUniquenessTest);
            AmqpConnection nonRecoverableConnection = await AmqpConnection.Factory.OpenConnectionAsync(addressUri);
            AmqpConnection recoverableConnection = await OpenTestConnectionAsync(addressUri);

            try
            {
                AmqpSession nonRecoverableSession1 = await nonRecoverableConnection.OpenSessionAsync(new AmqpSessionSettings());
                AmqpSession nonRecoverableSession2 = await nonRecoverableConnection.OpenSessionAsync(new AmqpSessionSettings());
                AmqpSession recoverableSession1 = await recoverableConnection.OpenSessionAsync(new AmqpSessionSettings());
                AmqpSession recoverableSession2 = await recoverableConnection.OpenSessionAsync(new AmqpSessionSettings());

                // Having duplicate link names on same connection is allowed without link recovery.
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
                terminus = sendingLink2.GetLinkTerminus();
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

        // Oasis AMQP doc section 3.4.6, example delivery tag 1.
        // Local sender has DeliveryState = null, remote receiver does not have this unsettled delivery.
        // Expected behavior is that the sender will immediately resend this delivery with field resume=false if settle mode is not settle-on-send.
        [Fact]
        public async Task ClientSenderNullDeliveryStateBrokerNoDeliveryStateTest()
        {
            await NegotiateUnsettledDeliveryTestAsync<SendingAmqpLink>(
                nameof(ClientSenderNullDeliveryStateBrokerNoDeliveryStateTest),
                true,
                null,
                false,
                null,
                true,
                true);
        }

        // Oasis AMQP doc section 3.4.6, example delivery tag 1 with sender/receiver swapped.
        // Local receiver has DeliveryState = null, remote sender does not have this unsettled delivery.
        // Expected behavior is that the sender will not be sending anything, the client side receiver should just remove this unsettled delivery.
        [Fact]
        public async Task ClientReceiverNullDeliveryStateBrokerNoDeliveryStateTest()
        {
            await NegotiateUnsettledDeliveryTestAsync<ReceivingAmqpLink>(
                nameof(ClientReceiverNullDeliveryStateBrokerNoDeliveryStateTest),
                true,
                null,
                false,
                null,
                false,
                false);
        }

        // Oasis AMQP doc section 3.4.6, example delivery tag 2
        // Local sender has DeliveryState = null, remote receiver has DeliveryState = Received.
        // Expected behavior is that the sender will immediately resend this delivery from the start with field resume=true.
        [Fact]
        public async Task ClientSenderNullDeliveryStateBrokerReceivedDeliveryStateTest()
        {
            await NegotiateUnsettledDeliveryTestAsync<SendingAmqpLink>(
                nameof(ClientSenderNullDeliveryStateBrokerReceivedDeliveryStateTest),
                true,
                null,
                true,
                AmqpConstants.ReceivedOutcome,
                true,
                false);
        }

        // Oasis AMQP doc section 3.4.6, example delivery tag 2 with sender/receiver swapped.
        // Local receiver has DeliveryState = null, remote sender has DeliveryState = Received.
        // Expected behavior is that the sender will immediately resend this delivery with resume=true and aborted=true.
        [Fact]
        public async Task ClientReceiverNullDeliveryStateBrokerReceivedDeliveryStateTest()
        {
            await NegotiateUnsettledDeliveryTestAsync<ReceivingAmqpLink>(
                nameof(ClientReceiverNullDeliveryStateBrokerNoDeliveryStateTest),
                true,
                null,
                true,
                AmqpConstants.ReceivedOutcome,
                true,
                false);
        }

        // Oasis AMQP doc section 3.4.6, example delivery tag 3.
        // Local sender has DeliveryState = null, remote receiver has reached terminal DeliveryState.
        // Expected behavior is that the sender will just settle the delivery locally with nothing being sent.
        [Fact]
        public async Task ClientSenderNullDeliveryStateBrokerTerminalDeliveryStateTest()
        {
            await NegotiateUnsettledDeliveryTestAsync<SendingAmqpLink>(
                nameof(ClientSenderNullDeliveryStateBrokerTerminalDeliveryStateTest),
                true,
                null,
                true,
                AmqpConstants.AcceptedOutcome,
                false,
                false);
        }

        // Oasis AMQP doc section 3.4.6, example delivery tag 3 with sender/receiver swapped. This is essentially the same as example delivery tag 14.
        // Local receiver has DeliveryState = null, remote sender has terminal DeliveryState.
        // Expected behavior is that the sender will just settle the delivery locally with nothing being sent.
        [Fact]
        public async Task ClientReceiverNullDeliveryStateBrokerTerminalDeliveryStateTest()
        {
            await NegotiateUnsettledDeliveryTestAsync<ReceivingAmqpLink>(
                nameof(ClientReceiverNullDeliveryStateBrokerTerminalDeliveryStateTest),
                true,
                null,
                true,
                AmqpConstants.AcceptedOutcome,
                true,
                false);
        }

        // Oasis AMQP doc section 3.4.6, example delivery tag 4.
        // Local sender has DeliveryState = null, remote receiver has DeliveryState = null.
        // Expected behavior is that the sender will immediately resend this delivery with field resume=true if settle mode is not settle-on-send.
        [Fact]
        public async Task ClientSenderNullDeliveryStateBrokerNullDeliveryStateTest()
        {
            await NegotiateUnsettledDeliveryTestAsync<SendingAmqpLink>(
                nameof(ClientSenderNullDeliveryStateBrokerNullDeliveryStateTest),
                true,
                null,
                true,
                null,
                true,
                true);
        }

        // Oasis AMQP doc section 3.4.6, example delivery tag 4 with sender/receiver swapped. This is essentially the same as example delivery tag 14.
        // Local receiver has DeliveryState = null, remote sender has DeliveryState = null.
        // Expected behavior is that the sender will immediately resend this delivery with field resume=true if settle mode is not settle-on-send.
        [Fact]
        public async Task ClientReceiverNullDeliveryStateBrokerNullDeliveryStateTest()
        {
            await NegotiateUnsettledDeliveryTestAsync<ReceivingAmqpLink>(
                nameof(ClientReceiverNullDeliveryStateBrokerNullDeliveryStateTest),
                true,
                null,
                true,
                null,
                true,
                true);
        }

        // Oasis AMQP doc section 3.4.6, example delivery tag 5.
        // Local sender has DeliveryState = Received, remote receiver DeliveryState does not exist.
        // Expected behavior is that the sender will immediately resend this delivery with field resume=false if settle mode is not settle-on-send.
        [Fact]
        public async Task ClientSenderReceivedDeliveryStateBrokerNoDeliveryStateTest()
        {
            await NegotiateUnsettledDeliveryTestAsync<SendingAmqpLink>(
                nameof(ClientSenderReceivedDeliveryStateBrokerNoDeliveryStateTest),
                true,
                AmqpConstants.ReceivedOutcome,
                false,
                null,
                true,
                true);
        }

        // Oasis AMQP doc section 3.4.6, example delivery tag 5 with sender/receiver swapped.
        // Local receiver has DeliveryState = Received, remote sender DeliveryState does not exist.
        // Expected behavior is that the sender will not be sending anything, the client side receiver should just remove this unsettled delivery.
        [Fact]
        public async Task ClientReceiverNoDeliveryStateBrokerReceivedDeliveryStateTest()
        {
            await NegotiateUnsettledDeliveryTestAsync<ReceivingAmqpLink>(
                nameof(ClientReceiverNoDeliveryStateBrokerReceivedDeliveryStateTest),
                true,
                AmqpConstants.ReceivedOutcome,
                false,
                null,
                false,
                false);

        }

        // Oasis AMQP doc section 3.4.6, example delivery tag 6, 7.
        // Local sender has DeliveryState = Received, remote receiver has DeliveryState = Received.
        // Expected behavior is that the sender will immediately resend this delivery from the start with field resume=true.
        [Fact]
        public async Task ClientSenderReceivedDeliveryStateBrokerReceivedDeliveryStateTest()
        {
            await NegotiateUnsettledDeliveryTestAsync<SendingAmqpLink>(
                nameof(ClientSenderReceivedDeliveryStateBrokerReceivedDeliveryStateTest),
                true,
                AmqpConstants.ReceivedOutcome,
                true,
                AmqpConstants.ReceivedOutcome,
                true,
                false);
        }

        // Oasis AMQP doc section 3.4.6, example delivery tag 6, 7 with sender/receiver swapped.
        // Local receiver has DeliveryState = Received, remote sender has DeliveryState = Received.
        // Expected behavior is that the sender will immediately resend this delivery from the start with field resume=true.
        [Fact]
        public async Task ClientReceiverReceivedDeliveryStateBrokerReceivedDeliveryStateTest()
        {
            await NegotiateUnsettledDeliveryTestAsync<ReceivingAmqpLink>(
                nameof(ClientReceiverReceivedDeliveryStateBrokerReceivedDeliveryStateTest),
                true,
                AmqpConstants.ReceivedOutcome,
                true,
                AmqpConstants.ReceivedOutcome,
                true,
                false);
        }

        // Oasis AMQP doc section 3.4.6, example delivery tag 8.
        // Local sender has DeliveryState = Received, remote receiver has reached terminal outcome.
        // Expected behavior is that the sender will just settle the delivery locally without resending the delivery.
        [Fact]
        public async Task ClientSenderReceivedDeliveryStateBrokerTerminalDeliveryStateTest()
        {
            await NegotiateUnsettledDeliveryTestAsync<SendingAmqpLink>(
                nameof(ClientSenderReceivedDeliveryStateBrokerTerminalDeliveryStateTest),
                true,
                AmqpConstants.ReceivedOutcome,
                true,
                AmqpConstants.AcceptedOutcome,
                false,
                false);
        }

        // Oasis AMQP doc section 3.4.6, example delivery tag 8 with sender/receiver swapped. This is essentially the same as example delivery tag 11.
        // Local receiver has DeliveryState = Received, remote sender has reached terminal outcome.
        // Expected behavior is that the sender will resend the delivery with resume=true and aborted=true.
        [Fact]
        public async Task ClientReceiverReceivedDeliveryStateBrokerTerminalDeliveryStateTest()
        {
            await NegotiateUnsettledDeliveryTestAsync<ReceivingAmqpLink>(
                nameof(ClientReceiverReceivedDeliveryStateBrokerTerminalDeliveryStateTest),
                true,
                AmqpConstants.ReceivedOutcome,
                true,
                AmqpConstants.AcceptedOutcome,
                true,
                false);

        }

        // Oasis AMQP doc section 3.4.6, example delivery tag 9.
        // Local sender has DeliveryState = Received, remote receiver has DeliveryState = null.
        // Expected behavior is that the sender will immediately resend this delivery with resume=true and aborted=true.
        [Fact]
        public async Task ClientSenderReceivedDeliveryStateBrokerNullDeliveryStateTest()
        {
            await NegotiateUnsettledDeliveryTestAsync<SendingAmqpLink>(
                nameof(ClientSenderReceivedDeliveryStateBrokerNullDeliveryStateTest),
                true,
                AmqpConstants.ReceivedOutcome,
                true,
                null,
                true,
                false);
        }

        // Oasis AMQP doc section 3.4.6, example delivery tag 9 with sender/receiver swapped. This is essentially the same as example delivery tag 2.
        // Local receiver has DeliveryState = Received, remote sender has DeliveryState = null.
        // Expected behavior is that the sender will immediately resend this delivery with resume=true and aborted=true.
        [Fact]
        public async Task ClientReceiverReceivedDeliveryStateBrokerNullDeliveryStateTest()
        {
            await NegotiateUnsettledDeliveryTestAsync<ReceivingAmqpLink>(
                nameof(ClientReceiverReceivedDeliveryStateBrokerNullDeliveryStateTest),
                true,
                AmqpConstants.ReceivedOutcome,
                true,
                null,
                true,
                false);
        }

        // Oasis AMQP doc section 3.4.6, example delivery tag 10
        // Local sender has terminal DeliveryState, remote receiver does not have this DeliveryState.
        // Expected behavior is that the sender will just settle the delivery locally without resending the delivery.
        [Fact]
        public async Task ClientSenderTerminalDeliveryStateBrokerNoDeliveryStateTest()
        {
            await NegotiateUnsettledDeliveryTestAsync<SendingAmqpLink>(
                nameof(ClientSenderTerminalDeliveryStateBrokerNoDeliveryStateTest),
                true,
                AmqpConstants.AcceptedOutcome,
                false,
                null,
                false,
                false);
        }

        // Oasis AMQP doc section 3.4.6, example delivery tag 10 with sender/receiver swapped.
        // Local receiver has terminal DeliveryState, remote sender does not have this DeliveryState.
        // Expected behavior is that the sender will not be sending anything, the client side receiver should just remove this unsettled delivery.
        [Fact]
        public async Task ClientReceiverTerminalDeliveryStateBrokerNoDeliveryStateTest()
        {
            await NegotiateUnsettledDeliveryTestAsync<ReceivingAmqpLink>(
                nameof(ClientReceiverTerminalDeliveryStateBrokerNoDeliveryStateTest),
                true,
                AmqpConstants.AcceptedOutcome,
                false,
                null,
                false,
                false);
        }

        // Oasis AMQP doc section 3.4.6, example delivery tag 11
        // Local sender has terminal DeliveryState, remote receiver has DeliveryState = Received.
        // Expected behavior is that the sender will resend the delivery with resume=true and aborted=true.
        [Fact]
        public async Task ClientSenderTerminalDeliveryStateBrokerReceivedDeliveryStateTest()
        {
            await NegotiateUnsettledDeliveryTestAsync<SendingAmqpLink>(
                nameof(ClientSenderTerminalDeliveryStateBrokerReceivedDeliveryStateTest),
                true,
                AmqpConstants.AcceptedOutcome,
                true,
                AmqpConstants.ReceivedOutcome,
                true,
                false);
        }

        // Oasis AMQP doc section 3.4.6, example delivery tag 11 with sender/receiver swapped. This is essentially the same as example delivery tag 8.
        // Local receiver has terminal DeliveryState, remote sender has DeliveryState = Received.
        // Expected behavior is that the sender will just settle the delivery locally without resending the delivery.
        [Fact]
        public async Task ClientReceiverTerminalDeliveryStateBrokerReceivedDeliveryStateTest()
        {
            await NegotiateUnsettledDeliveryTestAsync<ReceivingAmqpLink>(
                nameof(ClientReceiverTerminalDeliveryStateBrokerReceivedDeliveryStateTest),
                true,
                AmqpConstants.AcceptedOutcome,
                true,
                AmqpConstants.ReceivedOutcome,
                false,
                false);
        }

        // Oasis AMQP doc section 3.4.6, example delivery tag 12
        // Local sender has terminal DeliveryState, remote receiver has the same terminal DeliveryState.
        // Expected behavior is that the sender will resend the delivery with resume=true to settle the delivery.
        [Fact]
        public async Task ClientSenderTerminalDeliveryStateBrokerSameTerminalDeliveryStateTest()
        {
            await NegotiateUnsettledDeliveryTestAsync<SendingAmqpLink>(
                nameof(ClientSenderTerminalDeliveryStateBrokerSameTerminalDeliveryStateTest),
                true,
                AmqpConstants.AcceptedOutcome,
                true,
                AmqpConstants.AcceptedOutcome,
                true,
                false);
        }

        // Oasis AMQP doc section 3.4.6, example delivery tag 12 with sender/receiver swapped. 
        // Local receiver has terminal DeliveryState, remote sender has the same terminal DeliveryState.
        // Expected behavior is that the sender will resend the delivery with resume=true to settle the delivery.
        [Fact]
        public async Task ClientReceiverTerminalDeliveryStateBrokerSameTerminalDeliveryStateTest()
        {
            await NegotiateUnsettledDeliveryTestAsync<ReceivingAmqpLink>(
                nameof(ClientReceiverTerminalDeliveryStateBrokerSameTerminalDeliveryStateTest),
                true,
                AmqpConstants.AcceptedOutcome,
                true,
                AmqpConstants.AcceptedOutcome,
                true,
                false);
        }

        // Oasis AMQP doc section 3.4.6, example delivery tag 13
        // Local sender has terminal DeliveryState, remote receiver has the different terminal DeliveryState.
        // Expected behavior is that the sender will resend the delivery with resume=true and DeliveryState equal to the sender's DeliveryState to settle the delivery.
        [Fact]
        public async Task ClientSenderTerminalDeliveryStateBrokerDiffTerminalDeliveryStateTest()
        {
            await NegotiateUnsettledDeliveryTestAsync<SendingAmqpLink>(
                nameof(ClientSenderTerminalDeliveryStateBrokerDiffTerminalDeliveryStateTest),
                true,
                AmqpConstants.AcceptedOutcome,
                true,
                AmqpConstants.RejectedOutcome,
                true,
                false);
        }

        // Oasis AMQP doc section 3.4.6, example delivery tag 13 with sender/receiver swapped.
        // Local receiver has terminal DeliveryState, remote sender has the different terminal DeliveryState.
        // Expected behavior is that the sender will resend the delivery with resume=true and DeliveryState equal to the sender's DeliveryState to settle the delivery.
        [Fact]
        public async Task ClientReceiverTerminalDeliveryStateBrokerDiffTerminalDeliveryStateTest()
        {
            // Note: This test will actually fail if Released state is used instead of Rejected,
            // because broker will interpret it as actually releasing the lock on the message,
            // and resend the delivery again to the next available consumer, which is this test link (for a third time).
            await NegotiateUnsettledDeliveryTestAsync<ReceivingAmqpLink>(
                nameof(ClientReceiverTerminalDeliveryStateBrokerDiffTerminalDeliveryStateTest),
                true,
                AmqpConstants.AcceptedOutcome,
                true,
                AmqpConstants.RejectedOutcome,
                true,
                false);
        }

        // Oasis AMQP doc section 3.4.6, example delivery tag 14.
        // Local sender has terminal DeliveryState, remote receiver has null DeliveryState.
        // Expected behavior is that the sender will resend the delivery with resume=true and aborted=true.
        [Fact]
        public async Task ClientSenderTerminalDeliveryStateBrokerNullDeliveryStateTest()
        {
            await NegotiateUnsettledDeliveryTestAsync<SendingAmqpLink>(
                nameof(ClientSenderTerminalDeliveryStateBrokerNullDeliveryStateTest),
                true,
                AmqpConstants.AcceptedOutcome,
                true,
                null,
                true,
                false);
        }

        // Oasis AMQP doc section 3.4.6, example delivery tag 14 with sender/receiver swapped. This is essentially the same as example delivery tag 3.
        // Local receiver has terminal DeliveryState, remote sender has null DeliveryState.
        // Expected behavior is that the sender will just settle the delivery locally with nothing being sent.
        [Fact]
        public async Task ClientReceiverTerminalDeliveryStateBrokerNullDeliveryStateTest()
        {
            await NegotiateUnsettledDeliveryTestAsync<ReceivingAmqpLink>(
                nameof(ClientReceiverTerminalDeliveryStateBrokerNullDeliveryStateTest),
                true,
                AmqpConstants.AcceptedOutcome,
                true,
                null,
                false,
                false);
        }

        /// <summary>
        /// Test the negotiation of a single unsettled delivery between local and the remote peer.
        /// Please see the OASIS AMQP doc section 3.4.6 for the test scenarios.
        /// </summary>
        /// <typeparam name="T">The type of link that the local side will open towards remote (sending or receiving).</typeparam>
        /// <param name="testName">The name of the test. This will be used to set the link name as well as the queue name used during this test.</param>
        /// <param name="hasLocalDeliveryState">True if the local link unsettled map should have record of the unsettled delivery.</param>
        /// <param name="localDeliveryState">The actual value of the local unsettled delivery state.</param>
        /// <param name="hasRemoteDeliveryState">True if the remote link unsettled map should have record of the unsettled delivery.</param>
        /// <param name="remoteDeliveryState">The actual value of the local unsettled delivery state.</param>
        /// <param name="expectSend">True if the sender is expected to resend the unsettled delivery after negotiation with the receiver unsettled map.</param>
        /// <param name="testDiffSettleModes">True if the same test should be run again with link.SettleType = SettleMode.SettleOnSend (default is SettleMode.SettleOnReceive).</param>
        /// <returns></returns>
        static async Task NegotiateUnsettledDeliveryTestAsync<T>(
            string testName,
            bool hasLocalDeliveryState,
            DeliveryState localDeliveryState,
            bool hasRemoteDeliveryState,
            DeliveryState remoteDeliveryState,
            bool expectSend,
            bool testDiffSettleModes) where T : AmqpLink
        {
            TestAmqpConnection connection = await OpenTestConnectionAsync(addressUri);
            try
            {
                TestAmqpConnection brokerConnection = broker.FindConnection(connection.Settings.ContainerId) as TestAmqpConnection;
                TestAmqpConnection receiverSideConnection = typeof(T) == typeof(SendingAmqpLink) ? brokerConnection : connection;
                AmqpSession session = await connection.OpenSessionAsync();

                // Set up the unsettled message for both the local unsettled map and the remote unsettled map.
                var unsettledMap = new Dictionary<ArraySegment<byte>, Delivery>(ByteArrayComparer.Instance);
                var deliveryTag = new ArraySegment<byte>(Guid.NewGuid().ToByteArray());
                AmqpMessage localUnsettledMessage = hasLocalDeliveryState ? AddClientUnsettledDelivery(unsettledMap, deliveryTag, localDeliveryState) : null;
                AmqpMessage remoteUnsettledMessage = hasRemoteDeliveryState ? AddBrokerUnsettledDelviery($"{testName}1", deliveryTag, remoteDeliveryState) : null;
                AmqpMessage senderSideUnsettledMessage = typeof(T) == typeof(SendingAmqpLink) ? localUnsettledMessage : remoteUnsettledMessage;
                AmqpMessage receiverSideUnsettledMessage = typeof(T) == typeof(SendingAmqpLink) ? remoteUnsettledMessage : localUnsettledMessage;

                bool shouldSetResumeFlag = typeof(T) == typeof(SendingAmqpLink) ? hasRemoteDeliveryState : hasLocalDeliveryState;
                bool shouldSetAbortedFlag = (senderSideUnsettledMessage?.State is Received || senderSideUnsettledMessage?.State is Outcome)
                    && (typeof(T) == typeof(SendingAmqpLink) ? hasRemoteDeliveryState : hasLocalDeliveryState) 
                    && (receiverSideUnsettledMessage?.State is Received || receiverSideUnsettledMessage?.State == null);

                var localLink = await OpenTestLinkAsync<T>(session, $"{testName}1", unsettledMap);
                if (expectSend)
                {
                    if (!(receiverSideConnection.ReceivedPerformatives.Last.Value is Transfer))
                    {
                        string ex = "";
                        foreach (var performatve in receiverSideConnection.ReceivedPerformatives)
                        {
                            ex += performatve.GetType().ToString() + "\n";
                            if (performatve is Detach detach && detach.Error != null)
                            {
                                ex += detach.Error.ToString();
                            }
                        }
                        throw new ArgumentException(ex);
                    }
                    Assert.True(receiverSideConnection.ReceivedPerformatives.Last.Value is Transfer);
                    Assert.True(((Transfer)receiverSideConnection.ReceivedPerformatives.Last.Value).Resume == shouldSetResumeFlag);
                    Assert.True(((Transfer)receiverSideConnection.ReceivedPerformatives.Last.Value).Aborted == shouldSetAbortedFlag);
                    if (typeof(T) == typeof(SendingAmqpLink))
                    {
                        await TestReceivingMessageAsync(session, $"{testName}1", senderSideUnsettledMessage);
                    }
                }
                else
                {
                    Assert.True(receiverSideConnection.ReceivedPerformatives.Last.Value is Attach);
                    if (typeof(T) == typeof(SendingAmqpLink))
                    {
                        await TestReceivingMessageAsync(session, $"{testName}1", null);
                    }
                }

                await localLink.CloseAsync();
                if (testDiffSettleModes)
                {
                    // When settle mode is SettleMode.SettleOnSend, the client sender does not need to resend the message upon open.
                    unsettledMap = new Dictionary<ArraySegment<byte>, Delivery>(ByteArrayComparer.Instance);
                    deliveryTag = new ArraySegment<byte>(Guid.NewGuid().ToByteArray());
                    localUnsettledMessage = hasLocalDeliveryState ? AddClientUnsettledDelivery(unsettledMap, deliveryTag, localDeliveryState) : null;
                    remoteUnsettledMessage = hasRemoteDeliveryState ? AddBrokerUnsettledDelviery(testName, deliveryTag, remoteDeliveryState) : null;
                    await OpenTestLinkAsync<T>(session, $"{testName}2", unsettledMap, SettleMode.SettleOnSend);
                    Assert.True(receiverSideConnection.ReceivedPerformatives.Last.Value is Attach);
                    if (typeof(T) == typeof(SendingAmqpLink))
                    {
                        await TestReceivingMessageAsync(session, $"{testName}2", null);
                    }
                }
            }
            finally
            {
                connection?.Close();
            }
        }

        static async Task<TestAmqpConnection> OpenTestConnectionAsync(Uri addressUri)
        {
            AmqpConnectionFactory factory = new AmqpConnectionFactory();
            AmqpSettings settings = factory.GetAmqpSettings(null);
            TransportBase transport = await factory.GetTransportAsync(addressUri, settings, AmqpConstants.DefaultTimeout, CancellationToken.None);
            var connection = new TestAmqpConnection(transport, settings, new AmqpConnectionSettings() { ContainerId = Guid.NewGuid().ToString(), HostName = addressUri.Host, EnableLinkRecovery = true });
            await connection.OpenAsync();
            return connection;
        }

        static AmqpMessage AddClientUnsettledDelivery(Dictionary<ArraySegment<byte>, Delivery> unsettledMap, ArraySegment<byte> deliveryTag, DeliveryState deliveryState)
        {
            AmqpMessage message = AmqpMessage.Create("My Message");
            message.DeliveryTag = deliveryTag;
            message.State = deliveryState;
            unsettledMap.Add(message.DeliveryTag, message);
            return message;
        }

        static AmqpMessage AddBrokerUnsettledDelviery(string linkName, ArraySegment<byte> deliveryTag, DeliveryState deliveryState)
        {
            AmqpMessage message = new BrokerMessage(AmqpMessage.Create("My Message"));
            message.DeliveryTag = deliveryTag;
            message.State = deliveryState;
            broker.MockUnsettledReceivingDeliveries.AddOrUpdate(linkName, (key) => new List<Delivery>() { message },
                (key, unsettledDeliveries) =>
                {
                    unsettledDeliveries.Add(message);
                    return unsettledDeliveries;
                });

            return message;
        }

        static async Task<AmqpLink> OpenTestLinkAsync<T>(AmqpSession session, string linkName, Dictionary<ArraySegment<byte>, Delivery> unsettledMap, SettleMode settleMode = SettleMode.SettleOnReceive) where T : AmqpLink
        {
            Type linkType = typeof(T);
            AmqpLinkSettings linkSettings = new AmqpLinkSettings();
            linkSettings.Unsettled = new AmqpMap(
                unsettledMap.ToDictionary(
                    kvPair => kvPair.Key,
                    kvPair => kvPair.Value.State),
                ByteArrayComparer.MapKeyByteArrayComparer.Instance);

            if (linkType == typeof(SendingAmqpLink))
            {
                linkSettings.LinkName = linkName;
                linkSettings.Role = false;
                linkSettings.Source = new Source();
                linkSettings.Target = new Target() { Address = linkName };
            }
            else if (linkType == typeof(ReceivingAmqpLink))
            {
                linkSettings.LinkName = linkName;
                linkSettings.Role = true;
                linkSettings.Source = new Source() { Address = linkName };
                linkSettings.TotalLinkCredit = AmqpConstants.DefaultLinkCredit;
                linkSettings.AutoSendFlow = true;
                linkSettings.Target = new Target();
            }
            else
            {
                throw new NotSupportedException(linkType.Name);
            }

            linkSettings.SettleType = settleMode;
            var terminus = new AmqpLinkTerminus(linkSettings);
            terminus.UnsettledMap = unsettledMap;
            AmqpLink link = await session.RecoverLinkAsync<T>(terminus);
            await Task.Delay(1000); // wait for the sender to potentially send the initial deliveries
            return link;
        }

        /// <summary>
        /// Try receiving the message to verify that the message was indeed sent to the broker.
        /// </summary>
        /// <param name="session">The session used to create the test receiver.</param>
        /// <param name="expectedMessage">The expected message to be received. Null ifthere should be no message received.</param>
        /// <returns></returns>
        static async Task TestReceivingMessageAsync(AmqpSession session, string linkName, AmqpMessage expectedMessage)
        {
            var receiver = await session.OpenLinkAsync<ReceivingAmqpLink>($"{linkName}-testReceiver", linkName);
            try
            {
                AmqpMessage received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
                if (expectedMessage == null)
                {
                    Assert.Null(received);
                }
                else
                {
                    Assert.NotNull(received);
                    Assert.Equal(expectedMessage.ValueBody.Value, received.ValueBody.Value);
                    receiver.AcceptMessage(received);
                }
            }
            finally
            {
                await receiver.CloseAsync();
            }
        }
    }
}