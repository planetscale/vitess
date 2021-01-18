---
title: vtctl Resharding Throttler Command Reference
series: vtctl
docs_nav_title: Resharding Throttler
---

The following `vtctl` commands are available for administering Resharding Throttler.

## Commands

### ThrottlerMaxRates

Returns the current max rate of all active resharding throttlers on the server.

#### Example

<pre class="command-example">ThrottlerMaxRates -server &lt;vtworker or vttablet&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| server | string | vtworker or vttablet to connect to |


#### Arguments

* <code>&lt;vtworker or vttablet&gt;</code> &ndash; Required.

#### Errors

* the ThrottlerSetMaxRate command does not accept any positional parameters This error occurs if the command is not called with exactly 0 arguments.
* error creating a throttler client for <code>&lt;server&gt;</code> '%v': %v
* failed to get the throttler rate from <code>&lt;server&gt;</code> '%v': %v

### ThrottlerSetMaxRate

Sets the max rate for all active resharding throttlers on the server.

#### Example

<pre class="command-example">ThrottlerSetMaxRate -server &lt;vtworker or vttablet&gt; &lt;rate&gt;</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| server | string | vtworker or vttablet to connect to |


#### Arguments

* <code>&lt;vtworker or vttablet&gt;</code> &ndash; Required.
* <code>&lt;rate&gt;</code> &ndash; Required.

#### Errors

* the <code>&lt;rate&gt;</code> argument is required for the <code>&lt;ThrottlerSetMaxRate&gt;</code> command This error occurs if the command is not called with exactly one argument.
* failed to parse rate '%v' as integer value: %v
* error creating a throttler client for <code>&lt;server&gt;</code> '%v': %v
* failed to set the throttler rate on <code>&lt;server&gt;</code> '%v': %v


### GetThrottlerConfiguration

Returns the current configuration of the MaxReplicationLag module. If no throttler name is specified, the configuration of all throttlers will be returned.

#### Example

<pre class="command-example">GetThrottlerConfiguration -server &lt;vtworker or vttablet&gt; [&lt;throttler name&gt;]</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| server | string | vtworker or vttablet to connect to |


#### Arguments

* <code>&lt;vtworker or vttablet&gt;</code> &ndash; Required.
* <code>&lt;throttler name&gt;</code> &ndash; Optional.

#### Errors

* the <code>&lt;GetThrottlerConfiguration&gt;</code> command accepts only <code>&lt;throttler name&gt;</code> as optional positional parameter This error occurs if the command is not called with more than 1 arguments.
* error creating a throttler client for <code>&lt;server&gt;</code> '%v': %v
* failed to get the throttler configuration from <code>&lt;server&gt;</code> '%v': %v

### UpdateThrottlerConfiguration

Updates the configuration of the MaxReplicationLag module. The configuration must be specified as protobuf text. If a field is omitted or has a zero value, it will be ignored unless -copy_zero_values is specified. If no throttler name is specified, all throttlers will be updated.

#### Example

<pre class="command-example">UpdateThrottlerConfiguration `-server &lt;vtworker or vttablet&gt; [-copy_zero_values] "&lt;configuration protobuf text&gt;" [&lt;throttler name&gt;]`</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| copy_zero_values | Boolean | If true, fields with zero values will be copied as well |
| server | string | vtworker or vttablet to connect to |


#### Arguments

* <code>&lt;vtworker or vttablet&gt;</code> &ndash; Required.
* <code>&lt;throttler name&gt;</code> &ndash; Optional.

#### Errors

* Failed to unmarshal the configuration protobuf text (%v) into a protobuf instance: %v
* error creating a throttler client for <code>&lt;server&gt;</code> '%v': %v
* failed to update the throttler configuration on <code>&lt;server&gt;</code> '%v': %v


### ResetThrottlerConfiguration

Resets the current configuration of the MaxReplicationLag module. If no throttler name is specified, the configuration of all throttlers will be reset.

#### Example

<pre class="command-example">ResetThrottlerConfiguration -server &lt;vtworker or vttablet&gt; [&lt;throttler name&gt;]</pre>

#### Flags

| Name | Type | Definition |
| :-------- | :--------- | :--------- |
| server | string | vtworker or vttablet to connect to |


#### Arguments

* <code>&lt;vtworker or vttablet&gt;</code> &ndash; Required.
* <code>&lt;throttler name&gt;</code> &ndash; Optional.

#### Errors

* the <code>&lt;ResetThrottlerConfiguration&gt;</code> command accepts only <code>&lt;throttler name&gt;</code> as optional positional parameter This error occurs if the command is not called with more than 1 arguments.
* error creating a throttler client for <code>&lt;server&gt;</code> '%v': %v
* failed to get the throttler configuration from <code>&lt;server&gt;</code> '%v': %v


## See Also

* [vtctl command index](../../vtctl)
