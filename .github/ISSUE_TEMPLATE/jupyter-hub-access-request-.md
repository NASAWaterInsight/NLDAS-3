---
name: 'Jupyter Hub Access Request '
about: Describe this issue template's purpose here.
title: ''
labels: ''
assignees: ''

---

---
name: "AWS Access Request"
about: "Request access to AWS"
title: "[Access Request] <Your Name>"
labels: ["access-request"]
body:
  - type: input
    id: name
    attributes:
      label: "Full Name"
      placeholder: "Enter your full name"
    validations:
      required: true
  - type: input
    id: affiliation
    attributes:
      label: "Affiliation"
      placeholder: "Enter your affiliation"
    validations:
      required: true
  - type: input
    id: email
    attributes:
      label: "Email ID"
      placeholder: "Enter your email address"
    validations:
      required: true
  - type: textarea
    id: use_case
    attributes:
      label: "Use Case"
      placeholder: "Explain why you need jupyterhub access"
    validations:
      required: true
