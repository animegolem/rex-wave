---- MODULE MCMailMachines ----
EXTENDS MailMachines
MC_To == [l \in Letters |-> IF l = "l3" THEN "s2" ELSE "s1"]
====
