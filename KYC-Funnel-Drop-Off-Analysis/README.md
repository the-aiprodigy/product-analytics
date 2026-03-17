# **KYC ONBOARDING: FUNNEL DROP-OFF ANALYSIS**

## Project Overview

This repository contains the end-to-end data science workflow for analyzing the **Know Your Customer (KYC)** onboarding journey. In the fintech sector, KYC is the critical "activation gate." If a user fails to verify their identity, they cannot fully engage with the product i.e. fund accounts, access credit, or generate revenue.

**Goal:** Identify where users abandon the oboarding journey, why they leave, and provide data-backed recommendations to improve the **KYC Completion Rate**.

<img width="639" height="130" alt="image" src="https://github.com/user-attachments/assets/abd09db9-06e3-48b2-a5d9-9a7ed2d60b76" />

## 📂 Data Schema

The analysis is powered by five interconnected datasets:

| File | Description | Key Fields |
| --- | --- | --- |
| `fintech_users.csv` | User demographic and acquisition data. | `user_id`, `country`, `age`, `acquisition_source` |
| `fintech_devices.csv` | Hardware specifications for the user's primary device. | `user_id`, `os`, `device_model`, `camera_quality_score` |
| `network_logs.csv` | Telemetry regarding the user's connectivity during KYC. | `user_id`, `network_type`, `latency_ms`, `upload_speed_mbps` |
| `sessions.csv` | High-level session metadata. | `session_id`, `user_id`, `session_start`, `session_end` |
| `kyc_events.csv` | Granular event logs for every step of the KYC journey. | `session_id`, `event_name`, `status`, `error_code`, `timestamp` |

---

## The Funnel Stages

The analysis tracks users through the following 10-step linear path:

1. `start_kyc` ➔ 2. `phone_verification` ➔ 3. `personal_information` ➔ 4. `document_upload` ➔ 5. `document_validation` ➔ 6. `selfie_capture` ➔ 7. `face_match` ➔ 8. `address_verification` ➔ 9. `manual_review` ➔ 10. `kyc_approved`.

---

## Tech Stack

* **Language:** Python 3.10+
* **Analysis:** `Pandas`, `NumPy`
* **Visualization:** `Matplotlib`, `Seaborn`, `Plotly` (for interactive funnels)
* **Statistical Modeling:** `SciPy` or `Statsmodels` (for segment significance testing)

---

## Analysis Workflow

The project follows a standard Data Science Life Cycle:

1. **Data Audit:** Check for referential integrity (e.g., do all `kyc_events` have a matching `user_id` in the users table?).
2. **Feature Engineering:** Calculate `retry_count`, `step_duration`, and bucket technical metrics like `latency` into high/medium/low.
3. **Funnel Visualization:** Identify the "Great Wall of Friction" (the step with the highest % drop-off).
4. **Root Cause Analysis (RCA):** Correlate hardware (camera quality) and network (upload speed) with failure events.
5. **Segmentation:** Compare completion rates across iOS vs. Android, and Organic vs. Paid acquisition.
6. **Strategic Recommendations:** Propose UX/Technical changes and design experiments (A/B tests).

---

## Success Metrics

* **Primary:** KYC Completion Rate (CR).
* **Secondary:** Average Steps to Approval, Retry Rate per Step, and Time-to-Verify (TTV).

---

Copyright (c) 2026 Mapenzi Supaki

Shield: [![CC BY-NC 4.0][cc-by-nc-shield]][cc-by-nc]

This work is licensed under a
[Creative Commons Attribution-NonCommercial 4.0 International License][cc-by-nc].

[![CC BY-NC 4.0][cc-by-nc-image]][cc-by-nc]

[cc-by-nc]: https://creativecommons.org/licenses/by-nc/4.0/
[cc-by-nc-image]: https://licensebuttons.net/l/by-nc/4.0/88x31.png
[cc-by-nc-shield]: https://img.shields.io/badge/License-CC%20BY--NC%204.0-lightgrey.svg
