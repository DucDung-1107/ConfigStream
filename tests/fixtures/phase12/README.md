# Owned Terraform text fixture

Original ConfigStream test data; no third-party configuration copied.
Never execute these files. Tests construct an offline Git repository from the JSON.
It includes timestamp ties, unchanged commits, deletion/recreation, a rename and
an A-B-A content reversion. The deliberately fake secret marker must be rejected
for the entire affected incarnation; a later safe incarnation remains independent.
