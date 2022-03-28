kubectl patch deployment patch-demo --type merge --patch "$(cat patch-file-2.yaml)"
