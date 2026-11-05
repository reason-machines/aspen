# S5 wire examples

Excerpts from `requests.jsonl` (git 2.56.0 client, `capproxy.py` server).
Object IDs are from one run; ref lines are omitted from advertisements.

## v0 advertisement, stock (shallow and filter advertised)

Mode `stock`, `GET info/refs`, Git-Protocol `(v0)`.

advertisement:
```
# service=git-upload-pack
<flush>
caps: multi_ack thin-pack side-band side-band-64k ofs-delta shallow deepen-since deepen-not deepen-relative no-progress include-tag multi_ack_detailed allow-tip-sha1-in-want allow-reachable-sha1-in-want no-done symref=HEAD:refs/heads/main filter object-format=sha1 agent=git/2.39.5
<flush>
```

## v0 advertisement, strip (neither advertised)

Mode `strip`, `GET info/refs`, Git-Protocol `(v0)`.

advertisement:
```
# service=git-upload-pack
<flush>
caps: multi_ack thin-pack side-band side-band-64k ofs-delta no-progress include-tag multi_ack_detailed allow-tip-sha1-in-want allow-reachable-sha1-in-want no-done symref=HEAD:refs/heads/main object-format=sha1 agent=git/2.39.5
<flush>
```

## v2 advertisement, stock

Mode `stock`, `GET info/refs`, Git-Protocol `version=2`.

advertisement:
```
version 2
agent=git/2.39.5
ls-refs=unborn
fetch=shallow wait-for-done filter
server-option
object-format=sha1
object-info
<flush>
```

## v2 advertisement, nofilter (shallow only)

Mode `nofilter`, `GET info/refs`, Git-Protocol `version=2`.

advertisement:
```
version 2
agent=git/2.39.5
ls-refs=unborn
fetch=shallow wait-for-done
server-option
object-format=sha1
object-info
<flush>
```

## v2 `clone --depth 1` request and stock response

Mode `nofilter`, `POST git-upload-pack`, Git-Protocol `version=2`.

request:
```
command=fetch
agent=git/2.56.0-Linux
object-format=sha1
<delim>
thin-pack
no-progress
include-tag
ofs-delta
deepen 1
want 4f11d25aa8251079c1720c9db3c9a4fbcb16106d
want 4f11d25aa8251079c1720c9db3c9a4fbcb16106d
done
<flush>
```
response:
```
shallow-info
shallow 4f11d25aa8251079c1720c9db3c9a4fbcb16106d
<delim>
packfile
<flush>
```

## v2 `clone --filter=blob:none` against strip: no filter line is sent

Mode `strip`, `POST git-upload-pack`, Git-Protocol `version=2`.

request:
```
command=fetch
agent=git/2.56.0-Linux
object-format=sha1
<delim>
thin-pack
no-progress
ofs-delta
want 4f11d25aa8251079c1720c9db3c9a4fbcb16106d
want 4f11d25aa8251079c1720c9db3c9a4fbcb16106d
want 265815f299a4e1828d7edf02ceda4229cb04591c
want 73bf0822dd888f616b361b4e412b6a3859dec2bf
done
<flush>
```
response:
```
packfile
<flush>
```

## v2 shallow request refused with ERR

Mode `refuse`, `POST git-upload-pack`, Git-Protocol `version=2`.

request:
```
command=fetch
agent=git/2.56.0-Linux
object-format=sha1
<delim>
thin-pack
no-progress
include-tag
ofs-delta
deepen 1
want 4f11d25aa8251079c1720c9db3c9a4fbcb16106d
want 4f11d25aa8251079c1720c9db3c9a4fbcb16106d
done
<flush>
```
response:
```
ERR aspen: shallow fetches are not supported by this server; clone without --depth/--shallow-since/--shallow-exclude, or run 'git fetch --unshallow' against another remote first
```

## v2 `clone --depth 1` served by overfetch (full pack + exact shallow-info)

Mode `overfetch`, `POST git-upload-pack`, Git-Protocol `version=2`.

request:
```
command=fetch
agent=git/2.56.0-Linux
object-format=sha1
<delim>
thin-pack
no-progress
include-tag
ofs-delta
deepen 1
want 4f11d25aa8251079c1720c9db3c9a4fbcb16106d
want 4f11d25aa8251079c1720c9db3c9a4fbcb16106d
done
<flush>
```
overfetch:
```
{
 "shallow_info": [
  "shallow-info",
  "shallow 4f11d25aa8251079c1720c9db3c9a4fbcb16106d"
 ],
 "full_pack_bytes": 501739,
 "exact_shallow_pack_bytes": 17201
}
```

## v2 lazy fetch from a blob:none partial clone (wants are blob IDs, no filter line)

Mode `strip`, `POST git-upload-pack`, Git-Protocol `version=2`.

request:
```
command=fetch
agent=git/2.56.0-Linux
object-format=sha1
<delim>
thin-pack
no-progress
ofs-delta
want 0b26b949908890aa233d8ca946fcb1a45243cf4a
want 1be20796e03de1ccc5cf40954c113e91822a4abc
want 1cedb7323366c28600426896ab564649587636bb
want 5000f3b66665d096dfd0a82563714faa4e5e766b
want 7a6b0bb21afaa391ff396328d95fc57d1cb27e63
want 8d6ad1dbd77ee276ec754687e307385cc01c1a31
want 908911c8a43617cb5810a0f216207499b4930d68
want af93074da137acc6cf2d21817c678db532dc28a2
want dd3b37d078c804676870907ed5395ab02805df79
done
<flush>
```

