Data Mini #1 Competition README.md 

# Data-Mini-1
Welcome to Data Mini #1! You and your teammates will have 2 hours to explore and prepare a presentation about your data analysis. The hour after will be spent presenting your findings.

This Git repository contains this `README` file and the dataset in both `CSV` and `RDS` formats. This  `README` will include the dataset's dictionary, along with the rubric and policies. The `CSV` and the `RDS` have the same rows and columns, but the `RDS` will preserve R data types exactly (factors, dates, lists, etc.). We recommend you use Duke's Container Manager to access your preferred IDE. 

## How to Participate

1. Clone this repository
2. Load the data from `data/texasdata.csv` or `data/texasdata.rds`
3. Analyze the dataset and prepare a presentation of your findings
4. Submit your presentation slides/media to ctn16@duke.edu by Monday, February 9th at 11:59pm
Please do not attempt to push changes to this repository - it is read-only for participants.

## Data Dictionary
### Student-Teacher Race Match Dataset
#### About

Same-race teachers improve academic outcomes for minoritized students, yet most never encounter a teacher who shares their racial background. Existing research highlights the benefits of race-matched instruction but often focuses on aggregate workforce diversity, obscuring students’ structural opportunities for contact. QSIDE and students at Williams College introduce race-match sufficiency—the probability that a student has at least one same-race teacher—and estimate it using administrative data from 8,691 Texas public schools serving 5.4 million students in 2022-23.

`texasdata.rds` draws on three datasets, all accessed through TEA’s Texas Academic Performance Reports (TAPR) data portal (Texas Education Agency, 2023). The Campus Reference File (CREF) provides unique campus identifiers, structural characteristics, and campus type designations; its campus–district nesting reflects the governance structure that motivates our multilevel modeling approach. The Campus Profile File (CAMPPROF) reports student and teacher demographic counts by race and ethnicity, along with total enrollment and economic disadvantage measures. Finally, district classifications come from TEA’s District type, 2022–23 release, which applies National Center for Education Statistics (NCES) geographic categories (Texas Education Agency, 2024c). Merging these files yields a raw dataset of 9,044 campuses serving nearly 5.5 million students across 1,209 districts—a census of Texas public K–12 schools.

#### Data Variables
Data

`texasdata.rds`: Cleaned campus-level dataset derived from Texas Education Agency (TEA) TAPR data and NCES district classifications. Includes:
* Student and teacher race counts
* School-level demographics and grade spans
* Charter status and district type
* Economic disadvantage proportions

| variable | mode (in R) | description |
|----------|-------------|-------------|
| campusName | character | Name of the school campus |
| campusNumber | character | Unique identifier for the campus assigned by TEA |
| countyName | character | Name of the county where the campus is located |
| countyNumber | character | Numeric code identifying the county |
| districtName | character | Name of the school district to which the campus belongs |
| districtNumber | character | Unique identifier for the district assigned by TEA |
| region | factor | TEA-designated education service region |
| schoolLevel | factor | Level of schooling offered (e.g., elementary, middle, high school) |
| grade_lo | numeric | Lowest grade level served by the campus |
| grade_hi | numeric | Highest grade level served by the campus |
| charterSchool | logical | Indicator for whether the campus is a charter school (TRUE/FALSE) |
| locationType | factor | Geographic classification of the district based on NCES categories (e.g., urban, suburban, rural) |
| studentsTotal | numeric | Total student enrollment at the campus |
| teachersTotal | numeric | Total number of teachers at the campus |
| s_econ_disadv_prop | numeric | Proportion of students classified as economically disadvantaged |
| race | factor | Racial/ethnic category for demographic breakdowns |
| student_frac | numeric | Proportion of total students in a given racial/ethnic category |
| teacher_frac | numeric | Proportion of total teachers in a given racial/ethnic category |
| student_count | numeric | Number of students in a given racial/ethnic category |

# Competition Policies
## Code of Conduct
### AI and Online Sources
* AI tools and online resources are permitted with proper citations
* Include citations for forums (Stack Overflow), textbooks, and other references
* Provide a link to any AI chat conversations used
* You must list all citations (including the original study’s citation found at the bottom of this `README`) at the end of your code notebook

### Code Reuse
* You may reuse code from your own research or class projects
* You may NOT copy from the original student-teacher race match study - doing so will result in a major point deduction

## Analysis
* All types of data modeling are allowed
* All coding languages are allowed

## Submission Requirements
We will give teams ~5 minutes after the 2-hour analysis period to submit their code notebook (QMD, PDF, Jupyter, etc.) and presentation media (slides, PDF, etc.) to the Google Drive folder
* You and your teammates will have received an email from _______@duke.edu near the beginning of the competition start
* Your team’s Google Drive folder contents will not be accessible to view, comment, or edit by any other team

## Presentation Guidelines
* Present using any format: Quarto doc, slides, visualizations, etc: Your chosen presentation media will be projected by an SSMU member through your Google Drive
Presentations will be recorded during the event
Recordings will be sent to you and your teammates a few days following the event
SSMU would love to share your analysis with QSIDE and the original dataset creators! 
The dataset and study were created by previous undergraduate students at Williams College in 2023.
If you would like to opt into this opportunity, please let us know!
Presentation order will be randomized, but let us know beforehand if your team has a preference for going first, last, or in the middle
## Judging
Winners will be announced on Sunday, February 15
Presentations will be judged by the story your analysis tells, more than by the level of statistics your team members used.
This will account for differing experience levels across competitors
A fair evaluation will be justified by multiple people deciding on competition winners.
Insert rubric here?!?! ________
## Post-Competition
Duke’s StatSci Majors Union will be hosting Data Mini #2 from Feb. 28 - Mar. 1
This competition will be a little different: ________
In addition, the Duke Department of Statistical Science will host a larger competition, DataFest, from March 20-22, 2026

## Dataset Acknowledgment
The Student-Teacher Race-Match dataset and study were created by then-undergraduate students Anchal Bhaskar, Kayla Chang, Iman Najib, Olivia Thornton, Jules Tucher, and Régan Schwartz under the guidance of Professor Chad M. Topaz at Williams College.

This data is freely accessible online through the Institute for the Quantitative Study of Inclusion, Diversity, and Equity (QSIDE).

The citation for the replication package is:
Bhaskar, A., Chang, K., Najib, I., Thornton, O., Tucher, J., Schwartz, R., & Topaz, C. M. (2025). *Sufficiency in Teacher–Student Race Matching*. Manuscript

This citation must be included at the end of your code notebook.

## About This Event
Data Mini #1 was organized by Duke's Statistical Science Majors Union - Competitions and Opportunities Committee. Contributing members of the committee and the SSMU executive team are listed below:
Hanna Chee, ‘29
Trisha Iyer, ‘28
Shivansh Gupta, ‘28
Hyunjin Lee, ‘27
Susan Li, ‘29
Phillip Lin, ‘27
Liane Ma, ‘27
Chelsea Nguyen, ‘28
Cooper Ruffing, ‘27
Derek Wang, ‘28
Amy Xu, ‘27
Allison Yang, ‘27
And an additional thank-you goes out to the professors who helped advertise the event.

