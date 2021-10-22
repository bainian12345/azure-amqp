﻿//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:4.0.30319.42000
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace Microsoft.Azure.Amqp {
    using System;
    
    
    /// <summary>
    ///   A strongly-typed resource class, for looking up localized strings, etc.
    /// </summary>
    // This class was auto-generated by the StronglyTypedResourceBuilder
    // class via a tool like ResGen or Visual Studio.
    // To add or remove a member, edit your .ResX file then rerun ResGen
    // with the /str option, or rebuild your VS project.
    [global::System.CodeDom.Compiler.GeneratedCodeAttribute("System.Resources.Tools.StronglyTypedResourceBuilder", "16.0.0.0")]
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute()]
    [global::System.Runtime.CompilerServices.CompilerGeneratedAttribute()]
    internal class Resources {
        
        private static global::System.Resources.ResourceManager resourceMan;
        
        private static global::System.Globalization.CultureInfo resourceCulture;
        
        [global::System.Diagnostics.CodeAnalysis.SuppressMessageAttribute("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode")]
        internal Resources() {
        }
        
        /// <summary>
        ///   Returns the cached ResourceManager instance used by this class.
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        internal static global::System.Resources.ResourceManager ResourceManager {
            get {
                if (object.ReferenceEquals(resourceMan, null)) {
                    global::System.Resources.ResourceManager temp = new global::System.Resources.ResourceManager("Microsoft.Azure.Amqp.Resources", typeof(Resources).Assembly);
                    resourceMan = temp;
                }
                return resourceMan;
            }
        }
        
        /// <summary>
        ///   Overrides the current thread's CurrentUICulture property for all
        ///   resource lookups using this strongly typed resource class.
        /// </summary>
        [global::System.ComponentModel.EditorBrowsableAttribute(global::System.ComponentModel.EditorBrowsableState.Advanced)]
        internal static global::System.Globalization.CultureInfo Culture {
            get {
                return resourceCulture;
            }
            set {
                resourceCulture = value;
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Type {0} is not currently supported in AMQP application properties..
        /// </summary>
        internal static string AmqpApplicationProperties {
            get {
                return ResourceManager.GetString("AmqpApplicationProperties", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The buffer has already been reclaimed..
        /// </summary>
        internal static string AmqpBufferAlreadyReclaimed {
            get {
                return ResourceManager.GetString("AmqpBufferAlreadyReclaimed", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to A message cannot be cloned after it has been sent..
        /// </summary>
        internal static string AmqpCannotCloneSentMessage {
            get {
                return ResourceManager.GetString("AmqpCannotCloneSentMessage", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to A message cannot be sent because it is either received from a link or is already sent over a link..
        /// </summary>
        internal static string AmqpCannotResendMessage {
            get {
                return ResourceManager.GetString("AmqpCannotResendMessage", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to A link to connection &apos;{0}&apos; $cbs node has already been opened..
        /// </summary>
        internal static string AmqpCbsLinkAlreadyOpen {
            get {
                return ResourceManager.GetString("AmqpCbsLinkAlreadyOpen", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The session channel &apos;{0}&apos; cannot be found in connection &apos;{1}&apos;..
        /// </summary>
        internal static string AmqpChannelNotFound {
            get {
                return ResourceManager.GetString("AmqpChannelNotFound", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The connection was inactive for more than the allowed {0} milliseconds and is closed by container &apos;{1}&apos;..
        /// </summary>
        internal static string AmqpConnectionInactive {
            get {
                return ResourceManager.GetString("AmqpConnectionInactive", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The delivery id is already used (id:{0}, next-id:{1}, ulw:{2}, index:{3})..
        /// </summary>
        internal static string AmqpDeliveryIDInUse {
            get {
                return ResourceManager.GetString("AmqpDeliveryIDInUse", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The order {0} is defined more than once for type {1}. Please specify the Order property of AmqpMemberAttribute explicitly to avoid this error..
        /// </summary>
        internal static string AmqpDuplicateMemberOrder {
            get {
                return ResourceManager.GetString("AmqpDuplicateMemberOrder", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Dynamic terminus is not currently supported..
        /// </summary>
        internal static string AmqpDynamicTerminusNotSupported {
            get {
                return ResourceManager.GetString("AmqpDynamicTerminusNotSupported", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Sending an emtpy message is not allowed. At least one message section must be initialized..
        /// </summary>
        internal static string AmqpEmptyMessageNotAllowed {
            get {
                return ResourceManager.GetString("AmqpEmptyMessageNotAllowed", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The Encoding value of type ({0}:{1}) is different from that of its base type ({2}:{3})..
        /// </summary>
        internal static string AmqpEncodingTypeMismatch {
            get {
                return ResourceManager.GetString("AmqpEncodingTypeMismatch", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to An AMQP error occurred (condition=&apos;{0}&apos;)..
        /// </summary>
        internal static string AmqpErrorOccurred {
            get {
                return ResourceManager.GetString("AmqpErrorOccurred", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to No session-id was specified for a session receiver..
        /// </summary>
        internal static string AmqpFieldSessionId {
            get {
                return ResourceManager.GetString("AmqpFieldSessionId", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to A valid frame header cannot be formed from the incoming byte stream..
        /// </summary>
        internal static string AmqpFramingError {
            get {
                return ResourceManager.GetString("AmqpFramingError", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Global opaque (non-DNS) addresses are not supported (&apos;{0}&apos;).
        /// </summary>
        internal static string AmqpGlobalOpaqueAddressesNotSupported {
            get {
                return ResourceManager.GetString("AmqpGlobalOpaqueAddressesNotSupported", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Cannot allocate more handles. The maximum number of handles is {0}..
        /// </summary>
        internal static string AmqpHandleExceeded {
            get {
                return ResourceManager.GetString("AmqpHandleExceeded", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The handle &apos;{0}&apos; is already associated with object &apos;{1}&apos;..
        /// </summary>
        internal static string AmqpHandleInUse {
            get {
                return ResourceManager.GetString("AmqpHandleInUse", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The link handle &apos;{0}&apos; cannot be found in session &apos;{1}&apos;..
        /// </summary>
        internal static string AmqpHandleNotFound {
            get {
                return ResourceManager.GetString("AmqpHandleNotFound", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Idle timeout value specified in connection OPEN (&apos;{0} ms&apos;) is not supported. Minimum idle timeout is &apos;{1}&apos; ms..
        /// </summary>
        internal static string AmqpIdleTimeoutNotSupported {
            get {
                return ResourceManager.GetString("AmqpIdleTimeoutNotSupported", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to operation: {0}, state: {1}.
        /// </summary>
        internal static string AmqpIllegalOperationState {
            get {
                return ResourceManager.GetString("AmqpIllegalOperationState", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The requested data size &apos;{0}&apos; is greater than the length of the remaining buffer &apos;{1}&apos;..
        /// </summary>
        internal static string AmqpInsufficientBufferSize {
            get {
                return ResourceManager.GetString("AmqpInsufficientBufferSize", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Invalid command..
        /// </summary>
        internal static string AmqpInvalidCommand {
            get {
                return ResourceManager.GetString("AmqpInvalidCommand", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The format code &apos;{0}&apos; at frame buffer offset &apos;{1}&apos; is invalid or unexpected..
        /// </summary>
        internal static string AmqpInvalidFormatCode {
            get {
                return ResourceManager.GetString("AmqpInvalidFormatCode", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The hostname within the Target or Source address URL in the ATTACH frame (&apos;{0}&apos;) is different than the hostname passed in the connection OPEN (&apos;{1}&apos;)..
        /// </summary>
        internal static string AmqpInvalidLinkAttachAddress {
            get {
                return ResourceManager.GetString("AmqpInvalidLinkAttachAddress", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The scheme of the Target or Source address URL in the ATTACH frame (&apos;{0}&apos;) is invalid. Only &apos;amqp&apos; and &apos;amqps&apos; schemes are supported..
        /// </summary>
        internal static string AmqpInvalidLinkAttachScheme {
            get {
                return ResourceManager.GetString("AmqpInvalidLinkAttachScheme", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The message body type is invalid..
        /// </summary>
        internal static string AmqpInvalidMessageBodyType {
            get {
                return ResourceManager.GetString("AmqpInvalidMessageBodyType", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The format code &apos;{0}&apos; is not a valid message section code..
        /// </summary>
        internal static string AmqpInvalidMessageSectionCode {
            get {
                return ResourceManager.GetString("AmqpInvalidMessageSectionCode", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The descriptor code of the performative &apos;{0}&apos; cannot be processed by the link..
        /// </summary>
        internal static string AmqpInvalidPerformativeCode {
            get {
                return ResourceManager.GetString("AmqpInvalidPerformativeCode", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Invalid type for property &apos;{0}&apos; in the &apos;{1}&apos; section..
        /// </summary>
        internal static string AmqpInvalidPropertyType {
            get {
                return ResourceManager.GetString("AmqpInvalidPropertyType", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Invalid value for property &apos;{0}&apos; in the &apos;{1}&apos; section..
        /// </summary>
        internal static string AmqpInvalidPropertyValue {
            get {
                return ResourceManager.GetString("AmqpInvalidPropertyValue", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The operation cannot be performed against publisher &apos;{0}&apos; because the targeted namespace &apos;{1}&apos; is of {2} tier..
        /// </summary>
        internal static string AmqpInvalidPublisherOperationForMessagingSku {
            get {
                return ResourceManager.GetString("AmqpInvalidPublisherOperationForMessagingSku", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Unable to get valid source IP address for connection..
        /// </summary>
        internal static string AmqpInvalidRemoteIp {
            get {
                return ResourceManager.GetString("AmqpInvalidRemoteIp", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Open was already called for the object &apos;{0}&apos; (state={1})..
        /// </summary>
        internal static string AmqpInvalidReOpenOperation {
            get {
                return ResourceManager.GetString("AmqpInvalidReOpenOperation", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Comparison of {0} and {1} is invalid because the result is undefined..
        /// </summary>
        internal static string AmqpInvalidSequenceNumberComparison {
            get {
                return ResourceManager.GetString("AmqpInvalidSequenceNumberComparison", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The field {0} is either not set or not a supported type..
        /// </summary>
        internal static string AmqpInvalidType {
            get {
                return ResourceManager.GetString("AmqpInvalidType", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to A link with name &apos;{0}&apos; is already attached in session {1}..
        /// </summary>
        internal static string AmqpLinkNameInUse {
            get {
                return ResourceManager.GetString("AmqpLinkNameInUse", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Cannot open a {0} when the link settings represent a {1}..
        /// </summary>
        internal static string AmqpLinkOpenInvalidType {
            get {
                return ResourceManager.GetString("AmqpLinkOpenInvalidType", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to To enable link recovery, the {0} option on the connection must be set to true..
        /// </summary>
        internal static string AmqpLinkRecoveryNotEnabled {
            get {
                return ResourceManager.GetString("AmqpLinkRecoveryNotEnabled", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The link with link name {0} under connection with containerId {1} has been closed due to link stealing. .
        /// </summary>
        internal static string AmqpLinkStolen {
            get {
                return ResourceManager.GetString("AmqpLinkStolen", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to A link to connection &apos;{0}&apos; $management node has already been opened..
        /// </summary>
        internal static string AmqpManagementLinkAlreadyOpen {
            get {
                return ResourceManager.GetString("AmqpManagementLinkAlreadyOpen", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Management operation failed. status-code: {0}, status-description: {1}..
        /// </summary>
        internal static string AmqpManagementOperationFailed {
            get {
                return ResourceManager.GetString("AmqpManagementOperationFailed", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The received message (delivery-id:{0}, size:{1} bytes) exceeds the limit ({2} bytes) currently allowed on the link..
        /// </summary>
        internal static string AmqpMessageSizeExceeded {
            get {
                return ResourceManager.GetString("AmqpMessageSizeExceeded", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Property &apos;{0}&apos; in the &apos;{1}&apos; section is either missing, has the wrong type, or has an unexpected value..
        /// </summary>
        internal static string AmqpMissingOrInvalidProperty {
            get {
                return ResourceManager.GetString("AmqpMissingOrInvalidProperty", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Missing property &apos;{0}&apos; in the &apos;{1}&apos; section..
        /// </summary>
        internal static string AmqpMissingProperty {
            get {
                return ResourceManager.GetString("AmqpMissingProperty", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to None of the server sasl-mechanisms ({0}) are supported by the client ({1})..
        /// </summary>
        internal static string AmqpNotSupportMechanism {
            get {
                return ResourceManager.GetString("AmqpNotSupportMechanism", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to No valid IP addresses were found for host {0}..
        /// </summary>
        internal static string AmqpNoValidAddressForHost {
            get {
                return ResourceManager.GetString("AmqpNoValidAddressForHost", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The AMQP object {0} is aborted..
        /// </summary>
        internal static string AmqpObjectAborted {
            get {
                return ResourceManager.GetString("AmqpObjectAborted", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to {0} is not supported over AMQP..
        /// </summary>
        internal static string AmqpOperationNotSupported {
            get {
                return ResourceManager.GetString("AmqpOperationNotSupported", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to At least one protocol version must be set for {0}..
        /// </summary>
        internal static string AmqpProtocolVersionNotSet {
            get {
                return ResourceManager.GetString("AmqpProtocolVersionNotSet", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The requested protocol version &apos;{0}&apos; is not supported. The supported version is &apos;{1}&apos;..
        /// </summary>
        internal static string AmqpProtocolVersionNotSupported {
            get {
                return ResourceManager.GetString("AmqpProtocolVersionNotSupported", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to &apos;put-token&apos; operation: the &apos;name&apos; application property value &apos;{0}&apos; should match the token audience value. Expected: &apos;{1}&apos; .
        /// </summary>
        internal static string AmqpPutTokenAudienceMismatch {
            get {
                return ResourceManager.GetString("AmqpPutTokenAudienceMismatch", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Put token failed. status-code: {0}, status-description: {1}..
        /// </summary>
        internal static string AmqpPutTokenFailed {
            get {
                return ResourceManager.GetString("AmqpPutTokenFailed", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to A recoverable link with name &apos;{0}&apos; is already attached under connection with containerId {1}..
        /// </summary>
        internal static string AmqpRecoverableLinkNameInUse {
            get {
                return ResourceManager.GetString("AmqpRecoverableLinkNameInUse", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The required field &apos;{0}&apos; is not set in &apos;{1}&apos;..
        /// </summary>
        internal static string AmqpRequiredFieldNotSet {
            get {
                return ResourceManager.GetString("AmqpRequiredFieldNotSet", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The operation did not complete within the allocated time {0} for object {1}..
        /// </summary>
        internal static string AmqpTimeout {
            get {
                return ResourceManager.GetString("AmqpTimeout", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to There is no link credit to accept the delivery (id={0})..
        /// </summary>
        internal static string AmqpTransferLimitExceeded {
            get {
                return ResourceManager.GetString("AmqpTransferLimitExceeded", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The current transport is not secure. Establish a secure transport by setting AmqpTransportSettings.UseSslStreamSecurity to true..
        /// </summary>
        internal static string AmqpTransportNotSecure {
            get {
                return ResourceManager.GetString("AmqpTransportNotSecure", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to Cannot upgrade transport from type &apos;{0}&apos; to &apos;{1}&apos;..
        /// </summary>
        internal static string AmqpTransportUpgradeNotAllowed {
            get {
                return ResourceManager.GetString("AmqpTransportUpgradeNotAllowed", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The descriptor {0} is unknown while deserializing {1}. This usually happens if a wrong type is given to the ReadObject method or KnownTypesAttribute is not defined..
        /// </summary>
        internal static string AmqpUnknownDescriptor {
            get {
                return ResourceManager.GetString("AmqpUnknownDescriptor", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The object is not open..
        /// </summary>
        internal static string AmqpUnopenObject {
            get {
                return ResourceManager.GetString("AmqpUnopenObject", resourceCulture);
            }
        }
        
        /// <summary>
        ///   Looks up a localized string similar to The token type is not supported by this client implementation..
        /// </summary>
        internal static string AmqpUnsupportedTokenType {
            get {
                return ResourceManager.GetString("AmqpUnsupportedTokenType", resourceCulture);
            }
        }
    }
}
