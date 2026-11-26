---- MODULE MCMailHub ----
EXTENDS MailHub
MC_To == [l \in Letters |-> IF l = "l2" THEN "s2" ELSE "s1"]
====
