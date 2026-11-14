BEGIN { FS = "," }
{ sub(/\r$/, "") }
/^#/ || /^ID,/ { if (!(seen[substr($0, 1, 3)]++)) print; next }
$0 == "" { next }
$4 == "" { kinds["metadata"]++; print; next }
$22 != "" && ($2 == "E" || $2 == "I") && $22 + 0 > 0 {
    kinds["price"]++
    printf "%s,%s,%s,%s,,,,,,,,,,,,,,,,,,%s,,%s,,,%s,,,,,,,,,,,,,\n", $1, $2, $3, $4, $22, $24, $27
    next
}
$22 != "" { kinds["price_zero_or_invalid"]++; next }
$5 != "" || $7 != "" { kinds["quote"]++; next }
{
    empty = 1
    for (i = 5; i <= NF; i++) if ($i != "") { empty = 0; break }
    kinds[empty ? "housekeeping" : "other"]++
}
END { for (k in kinds) printf "%s %d\n", k, kinds[k] > "/dev/stderr" }
