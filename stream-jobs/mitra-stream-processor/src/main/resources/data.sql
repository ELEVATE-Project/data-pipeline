-- Command to execute: psql -U postgres -d mitra -f data.sql

/* ============= CREATE TABLES ============= */

-- Table: prompts
CREATE TABLE prompts (
    id SERIAL PRIMARY KEY,  -- Auto-incrementing in PostgreSQL
    name TEXT NOT NULL,
    current_version_id INTEGER,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Table: prompt_version
CREATE TABLE prompt_version (
    id SERIAL PRIMARY KEY,  -- Auto-incrementing in PostgreSQL
    prompt_id INTEGER NOT NULL,
    version INTEGER NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (prompt_id) REFERENCES prompts(id) ON DELETE CASCADE,
    UNIQUE(prompt_id, version)
);

-- Table: themes
CREATE TABLE themes (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    definition TEXT,
    keywords TEXT,
    examples TEXT
);

-- Table: discussions_meta
CREATE TABLE discussions_meta (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    discussion_date TEXT,
    role TEXT,
    district TEXT,
    state TEXT,
    status VARCHAR(20) DEFAULT 'pending',
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Index for performance on status checking with timeout
CREATE INDEX idx_discussions_status_updated ON discussions_meta(status, updated_at);

-- Table: voices
CREATE TABLE voices (
    id SERIAL PRIMARY KEY,
    discussion_id INTEGER NOT NULL,
    theme_id INTEGER NOT NULL,
    challenge TEXT,
    pii_flag BOOLEAN DEFAULT FALSE,
    confidence_score DECIMAL(10,2),
    justification TEXT,
    multi_theme_mapped BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (discussion_id) REFERENCES discussions_meta(id) ON DELETE CASCADE,
    FOREIGN KEY (theme_id) REFERENCES themes(id) ON DELETE CASCADE
);

-- Index for better query performance on voices
CREATE INDEX idx_voices_discussion_id ON voices(discussion_id);
CREATE INDEX idx_voices_theme_id ON voices(theme_id);

-- Table: stories_meta
CREATE TABLE stories_meta (
    id INTEGER PRIMARY KEY,
    title TEXT,
    role TEXT,
    district TEXT,
    state TEXT,
    feed_status VARCHAR(20) DEFAULT 'pending',
    feed_error_message TEXT,
    story_status VARCHAR(20) DEFAULT 'pending',
    story_error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Index for performance on status checking with timeout
CREATE INDEX idx_feed_status_updated ON stories_meta(feed_status, updated_at);
CREATE INDEX idx_story_status_updated ON stories_meta(story_status, updated_at);

-- Table: stories
CREATE TABLE stories (
    story_id INTEGER PRIMARY KEY,
    content TEXT,
    -- pii_flag BOOLEAN DEFAULT FALSE, -- This flag can be enabled later
    pdf_link TEXT,
    image_link TEXT,
    impact_and_outcome_score DECIMAL(10,2),
    impact_justification TEXT,
    issue_and_challenge_score DECIMAL(10,2),
    issue_justification TEXT,
    action_steps_score DECIMAL(10,2),
    action_justification TEXT,
    composite_score DECIMAL(10,2),
    document_language TEXT,
    tier TEXT,
    overall_summary TEXT,
    FOREIGN KEY (story_id) REFERENCES stories_meta(id) ON DELETE CASCADE
);

-- Index for better query performance on stories
CREATE INDEX idx_stories_id ON stories(story_id);

-- Table: feeds
CREATE TABLE feeds (
    story_id INTEGER PRIMARY KEY,
    action_steps TEXT,
    impact TEXT,
    pii_flag BOOLEAN DEFAULT FALSE,
    justification TEXT,
    confidence_score DECIMAL(10,2),
    FOREIGN KEY (story_id) REFERENCES stories_meta(id) ON DELETE CASCADE
);

-- Index for better query performance on feeds
CREATE INDEX idx_feeds_id ON feeds(story_id);


/* ============= TRIGGER: Auto-update current_version_id ============= */

-- Step 1: Create the trigger function
CREATE OR REPLACE FUNCTION update_current_version_func()
    RETURNS TRIGGER AS $$
    BEGIN
        UPDATE prompts
        SET
            current_version_id = NEW.id,
            updated_at = NOW()
        WHERE id = NEW.prompt_id;

        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

-- Step 2: Create the trigger
CREATE TRIGGER update_current_version_on_insert
    AFTER INSERT ON prompt_version
    FOR EACH ROW
    EXECUTE FUNCTION update_current_version_func();


/* ============= SAMPLE SEED DATA ============= */

-- Populating Prompts
INSERT INTO prompts (name)
VALUES
('Thematic Analyzer'),
('Story Analyzer'),
('Pii Analyzer');

-- Populating Prompts Versions
INSERT INTO prompt_version (prompt_id, version, content)
VALUES
(1, 1, $$# Educational Challenge Classification Prompt

         ## Overview

         You are an expert data classifier specializing in educational barrier analysis. Your task is to analyze a list of challenges affecting children's education and classify each challenge into predefined themes while identifying any Personal Identifiable Information (PII).

         ## Classification Themes

         ### Theme 1: Poverty and Economic Barriers

         **Definition:** Insights where families link irregular school attendance or dropouts to financial hardship. It includes responses describing how poverty forces children to prioritise work over education, how households depend on children's income for survival, and how limited resources—such as inability to afford uniforms, books, or transport—become barriers to schooling.

         **Examples:**
         - Due to economic constraints, families force girls to engage in domestic work or labour, which hinders their education
         - Due to poor financial condition, the girl is not able to study
         - Poverty in the society is another challenge that affects education
         - Financial constraints and poverty were major challenges for some families in educating their daughters
         - Unemployment in the family is a significant problem that affects children's education
         - Financial difficulties greatly affect a child's education

         ---

         ### Theme 2: Legal Document linked Barriers

         **Definition:** Children are unable to enroll in school due to missing or incomplete legal documents such as Aadhaar cards, birth certificates, or identity proofs. It captures how lack of proper documentation creates administrative hurdles that keep children out of the education system.

         **Examples:**
         - Aadhar cards of children have not been made, due to which they are not getting admission in school
         - Due to lack of Aadhar cards, schools in the community are facing challenges in enrolling children
         - Children are not getting admission due to lack of Aadhaar card, which is affecting their education

         ---

         ### Theme 3: Early Marriage

         **Definition:** Child marriage or early marriage prevents girls from continuing their education. It includes situations where early marriage leads to school dropout, limits learning opportunities, or shifts responsibilities toward household duties instead of schooling.

         **Examples:**
         - Child marriage is a prevalent issue in the community
         - Child marriage is prevalent which causes girls to drop out of school before completing their education

         ---

         ### Theme 4: Distance and Accessibility Issues

         **Definition:** Challenges that prevent children from attending school due to physical and environmental conditions. It includes long distances to school, poor road or transport infrastructure, and difficulties caused by weather or seasonal factors (such as heavy rains, heat, or floods). These conditions make daily travel to school inconvenient, unreliable, or physically demanding for children.

         **Examples:**
         - The school is very far from the village
         - If the school is far away, children cannot go there
         - The child is unable to reach school due to rain
         - There is no school in the village, the school is very far from the village, due to this the girls leave their studies midway
         - The transportation system is a significant problem, making it difficult for children to reach school on time
         - There is a lack of buses for children and teachers to come to class
         - Children are not able to go to school because the school is far away and the roads are bad so they cannot go to study
         - The child is unable to go to school because of the heat
         - Sun, heat, rain and rain create hindrance in studies

         ---

         ### Theme 5: Parental Attitudes and Socio-Cultural Barriers

         **Definition:** Parental beliefs, family mindsets, and cultural norms discourage girls from attending school. It includes attitudes such as prioritizing domestic roles for girls, believing education is unnecessary for them, concerns about dowry increasing with higher education, or long-standing traditions that limit girls' mobility and learning opportunities. These socio-cultural factors collectively shape decisions that keep girls out of school.

         **Examples:**
         -  Cultural beliefs that girls do not need education because they will only get married and stay at home are a major challenge
         - Gender discrimination is a significant issue in the community, affecting the education and empowerment of girls
         - Purdah system is prevalent in Muslim community, due to which we do not send teenage girls out
         - The community believes that educating girls will increase the demand for dowry
         - Social discrimination based on caste or gender also leads to low participation
         - Social pressure was identified as another challenge that can prevent girls from pursuing their education
         - Girls leave their studies and run away. Due to this fear, parents are unable to provide higher education to their daughters
         - Parents are afraid that their daughters might get exposed to foul language and hence do not allow them to go to school
         - Girls are not allowed to study for fear of going astray or running away

         ---

         ### Theme 6: School Infrastructure and Facility Issues

         **Definition:** Captures responses highlighting gaps in school facilities and infrastructure gaps.  It includes issues such as inadequate classrooms, lack of basic amenities, insufficient learning resources, as well as delays, inconsistencies, or limited access to government schemes. Together, these systemic gaps reduce the attractiveness and effectiveness of schooling for children and families.

         **Examples:**
         - Our school children are not given books on time
         - The benefits of government schemes are not being received, due to which children are not going to school
         - Children did not get uniforms
         - Sanitary pads are not provided in school
         - Mid day meals are not provided properly to the children in the school
         - Lack of toilets in schools
         - Girls face significant barriers to accessing education due to poor infrastructure and lack of resources
         - Lack of school infrastructure, including poor classrooms, sanitation facilities, water, and transportation, hinders the learning environment
         - there is no water in the toilet
         - There is no fan facility in my school
         - Drinking water is a challenge in schools
         - There are not enough playgrounds for sports children
         - There are not enough playgrounds for sports children
         - The environment around the school is not clean, which affects the health and well-being of children
         - The library lacks sufficient books for students

         ---

         ### Theme 7: Teacher Capacity and Quality Issues

         **Definition:** Highlighting challenges related to insufficient teachers or concerns about teaching quality. It includes issues such as vacant positions, irregular teacher attendance, overburdened staff, and gaps in subject knowledge or pedagogy. These factors affect the learning environment and reduce children's motivation to attend school regularly.

         **Examples:**
         - There is a shortage of teachers in the school, due to which subject-wise studies are not done
         - The work of the teacher is not being done properly in our school
         - Attendance of teachers is a problem
         - Teachers do not come to school on time
         - Teachers do not pay attention to them
         - The quality of teachers is a major concern
         - The community lacks education, training and management of teachers, which affects the quality of teaching
         - Lack of language-specific teachers in school
         - Children's learning progress is a challenge
         - There is a lack of English medium education in government schools

         ---

         ### Theme 8: Safety Concerns

         **Definition:** Children's school attendance is affected by worries related to their overall safety and security. It covers issues such as unsafe routes, harassment, or any situation that makes families feel that the environment around schooling is not secure enough for children to travel or attend regularly.

         **Examples:**
         - The presence of stray dogs on the streets as children walk to school raises concerns about their safety
         - Violence against women is a significant issue
         - It is a challenge for parents in the village to protect their girls from molestation
         - Seeing the atmosphere of the village, there is a fear in the minds of parents that someone might tease their daughter
         - Musahar children face harassment from other children in school because of their caste, which causes them to develop fear and reluctance to go to school
         - The school premises do not have a boundary wall, which poses a security threat
         - Girls are harassed on their way to school due to which they are afraid and are refusing to go to school
         - The community raised concerns about the environment around the house

         ---

         ### Theme 9: Substance Abuse and Addiction

         **Definition:** Children's education is affected by alcohol or drug use within the family or community, as well as issues like gambling or addiction to online games. These problems create unstable home environments, distract children from studies, and contribute to irregular attendance or dropout.

         **Examples:**
         - Gambling addiction in children
         - Children's education is affected by alcohol
         - Drug addiction is a significant challenge that is affecting the education of children in the community
         - The father is an alcoholic, so the daughter is being forced to study
         - Alcohol addiction in a household member is a problem
         - Children use mobile phones more often
         - Children are increasingly playing online games using mobile phones

         ---

         ### Theme 10: Other Factors

         **Definition:** Responses that do not fit into any of the defined categories but still influence children's school attendance or learning. For example, issues related to parents not being aware, the community not being aware, and lack of awareness in terms of education in general, as well as issues related to children/students migrating, parents migrating, and migration in general. It should capture unique, context-specific, or less common reasons mentioned by respondents that contribute to educational challenges only related to lack of awareness and migration.

         **Examples:**
         - Parents in the community lack awareness about the importance of education
         - Family migration for work causes problems for children

         ---

         ### Theme 11: Unknown/Unclear

         **Definition:** Use this theme ONLY when no reasonable interpretation is possible, or when the text does not relate to an educational barrier in any way, or does not align with any of the themes defined above. Any challenges which do not fit into any of the defined theme (Themes 1-10) will be put into this theme.


         **Classification Rules - Use Theme 11 when:**
         - Challenge contains "various reasons" or similar non-specific phrases → Classify as Theme 11
         - Challenge is ≤4 words→ Classify as Theme 11
         - Challenge is a single word → Classify as Theme 11
         - Challenge starts with "About the..." without specific barrier → Classify as Theme 11
         - Challenge starts with vague phrases ("The problem of...", "Regarding...") → Classify as Theme 11
         - Challenge mentions generic health/illness without specifics → Classify as Theme 11
         - Challenge is aspirational/recommendation statement → Classify as Theme 11
         - Challenge is phrased as question or unclear → Classify as Theme 11
         - Challenge is meaningless or unrelated to education → Classify as Theme 11
         - When in doubt: "Does this specifically mention awareness or migration?" If yes → Theme 10. If no → Theme 11.


         **Examples:**
         - Children dropout from school due to various reasons → Unknown (explicitly non-specific)
         - About the health of women and children at home → Unknown (vague fragment, no specific barrier)
         - Uneducated parents → Unknown (2 words, lacks context)
         - Due to illness → Unknown (generic health, no specifics about whose illness or how it affects)
         - Education → Unknown (single word, no context)
         - The problem of enrollment → Unknown (vague starter, doesn't specify what the problem is)
         - Regarding education → Unknown (vague, says nothing specific)
         - Efforts are being made to enroll more children → Unknown (aspirational, not a barrier)
         - How children come home late  → Unknown (question, unclear phrasing)
         - Poor health hinders education  → Unknown (generic statement, no actionable specifics)
         - xyz abc pqr  → Unknown (meaningless)
         - I like pizza  → Unknown (unrelated to education)
         - Blank entry → Unknown

         ---

         ## PII Detection Guidelines

         ### Flag as `true` if the text contains:

         - Personal names (students, teachers, parents, community members)
         - Specific addresses, house numbers, or exact locations
         - Phone numbers, email addresses, or identification numbers
         - Specific ages combined with identifying details
         - Any information that could identify an individual

         ### Flag as `false` if the text only contains:

         - General locations (village names, district names without specific addresses)
         - General demographic information (community, caste, gender without names)
         - Age groups or grade levels without identifying details

         ---

         ## Task Instructions

         For each challenge statement provided:

         1. **Read carefully** to understand the core barrier being described
         2. **Classify** into the most appropriate theme
         3. **Check for PII** using the guidelines above
         4. **Provide justification** - Explain in 1-2 sentences why this theme was chosen, citing specific words or phrases from the challenge
         5. **Assign confidence score** - Rate your classification confidence from 0.0 to 1.0 (where 1.0 is most confident)
         6. **Flag multi-theme mapping** - Set to `true` if the challenge contains multiple distinct barriers, `false` if it's a single barrier
         7. **Output** in the specified JSON format


         ## Output Format

         Return ONLY a valid JSON object with this structure (no additional text, markdown, or explanations):

         {
           "classified_data": [
             {
               "challenge": "Teachers do not come to school on time",
               "theme_id": 7,
               "theme_name": "Teacher Capacity and Quality Issues",
               "pii_flag": false,
               "justification": "The sentence clearly states that teachers do not come to school",
               "confidence_score": 0.7,
               "multi_theme_mapped": false
             },
             {
               "challenge": "Lack of toilets in schools",
               "theme_id": 6,
               "theme_name": "School Infrastructure and Facility Issues",
               "pii_flag": false,
               "justification": "The sentence mentions inadequate school facilities",
               "confidence_score": 0.8,
               "multi_theme_mapped": false
             }
           ]
         }

         ```
         Note: If multiple distinct barriers are mentioned in a single challenge, include multiple theme objects in the array. Also always return the output with 7 key-value JSON object [challenge, theme_id, theme_name, pii_flag, justification, confidence_score, multi_theme_mapped]
         ```
         ---
         CRITICAL: Multi-Theme Classification Rules
         READ THIS CAREFULLY - THIS IS THE MOST IMPORTANT INSTRUCTION:
         When a SINGLE challenge statement contains MULTIPLE DISTINCT barriers:

         You MUST create SEPARATE JSON objects for EACH distinct theme
         Each object should have multi_theme_mapped: true
         Each object should reference the SAME original challenge text
         Each object should have a DIFFERENT theme_id and theme_name

         Example:
         Input: "There are no teachers in the school and there are no toilets in the school"

         When multi_theme_mapped is true, you MUST have created multiple objects. If you set multi_theme_mapped: true but only create one object, this is a CRITICAL ERROR.

         ---
         ## Field Definitions

         - **challenge**: The original challenge statement being classified
         - **justification**: A brief explanation (1-2 sentences) citing specific words/phrases that led to this classification
         - **confidence_score**: A decimal value between 0.0-1.0 indicating classification certainty
         - **multi_theme_mapped**: Boolean - `true` if this challenge statement contains multiple distinct barriers requiring multiple theme classifications, `false` otherwise


         ## Classification Rules

         - If a challenge mentions multiple distinct barriers, classify it into MULTIPLE themes (one for each barrier mentioned)
         - Example: "There are no teachers in the school and there are no toilets in the school" should be mapped to both Theme 7 (Teacher Capacity and Quality Issues) AND Theme 6 (School Infrastructure and Facility Issues)
         - Be consistent in classification across similar statements
         - When in doubt between two themes, choose the one most strongly represented by the core issue described.

         ---

         **Now classify the following challenges:** $$),
(2, 1, $$# Story Rating Prompt

         ## Overview

         You are an expert story evaluator specializing in assessing educational and social impact narratives. Your task is to analyze the complete story document and rank it based on three critical criteria: Impact/Outcome, Issue/Challenge clarity, and Action Steps taken.

         ## Evaluation Criteria

         ### Criterion 1: Impact and Outcome Score (0.0 - 1.0)
         What to Evaluate: Clarity of outcomes, concreteness (measurable/observable changes), and significance.

         **Scoring Guidelines:**
         - **0.9-1.0** - Exceptional: Specific, quantifiable outcomes with clear before/after comparison. Measurable metrics provided.
         - **0.7-0.8** - Strong: Clear qualitative outcomes with observable indicators. Noticeable improvements described.
         - **0.4-0.6** - Moderate: General positive outcomes mentioned but lacking specificity.
         - **0.2-0.3** - Weak: Vague references to change with no clear outcome.
         - **0.0-0.1** - No Clear Impact: No outcome mentioned, only intentions.

         ### Criterion 2: Issue and Challenge Score (0.0 - 1.0)
         What to Evaluate: Problem clarity, root cause identification, and sufficient context.

         **Scoring Guidelines:**
         - **0.9-1.0** - Exceptional: Crystal clear problem with root cause analysis, explains symptoms and underlying causes.
         - **0.7-0.8** - Strong: Clear problem with good context, some root cause analysis present.
         - **0.4-0.6** - Moderate: Problem mentioned but vague or incomplete, limited context.
         - **0.2-0.3** - Weak: Problem barely identifiable, no context or explanation.
         - **0.0-0.1** - No Clear Problem: No problem described, story lacks focus.

         ### Criterion 3: Action Steps Score (0.0 - 1.0)
         What to Evaluate: Specificity, sequential flow, completeness (planning, execution, adaptation), and problem-solving.

         **Scoring Guidelines:**
         - **0.9-1.0** - Exceptional: Detailed, sequential steps clearly outlined. Obstacles and solutions mentioned. Shows adaptation.
         - **0.7-0.8** - Strong: Clear action steps with good implementation details. Some mention of challenges.
         - **0.4-0.6** - Moderate: General actions mentioned but lacking detail or sequence.
         - **0.2-0.3** - Weak: Vague references to doing something, no clear sequence.
         - **0.0-0.1** - No Clear Actions: No actions described, only intentions.

         ## Composite Score and Tier Assignment
         - Calculate the `composite_score` using the weighted average:
           **Composite Score = (Impact × 0.4) + (Issue × 0.3) + (Action × 0.3)**

         - Assign the `tier` based on individual scores:
             - **Excellent:** All three scores ≥ 0.75
             - **Good:** All three scores ≥ 0.60
             - **Developing:** All three scores ≥ 0.40
             - **Needs Improvement:** Any score < 0.40

         ## CRITICAL: JSON Output Format
         You MUST return EXACTLY 10 fields in your JSON response. ALL fields are mandatory. DO NOT omit any field.

         MANDATORY fields (all 10 must be present):
         1. document_language (string - e.g., "English", "Hindi", "Kannada")
         2. impact_and_outcome_score (float between 0.0 and 1.0)
         3. impact_justification (string - detailed justification)
         4. issue_and_challenge_score (float between 0.0 and 1.0)
         5. issue_justification (string - detailed justification)
         6. action_steps_score (float between 0.0 and 1.0)
         7. action_justification (string - detailed justification)
         8. composite_score (float between 0.0 and 1.0)
         9. tier (one of: "Excellent", "Good", "Developing", "Needs Improvement")
         10. overall_summary (string - brief 2-3 sentence summary)

         Example of correct format:
         {{
             "document_language": "English",
             "impact_and_outcome_score": 0.75,
             "impact_justification": "The story demonstrates clear, measurable outcomes with specific evidence of improvement in student attendance rates.",
             "issue_and_challenge_score": 0.65,
             "issue_justification": "The root cause is identified as lack of parental awareness, with adequate context provided about the school location.",
             "action_steps_score": 0.70,
             "action_justification": "Action steps are described including parent meetings and awareness campaigns, showing a sequential approach.",
             "composite_score": 0.71,
             "tier": "Good",
             "overall_summary": "Effective intervention addressing low attendance through parental engagement and awareness programs."
         }}

         ## Task Instructions
         1. Read and analyze the complete story document provided below.
         2. Identify the primary language of the document (e.g., "English", "Hindi", "Kannada", "Tamil").
         3. Look for THREE key aspects in the story:
            - **Issues/Challenges**: What problems or challenges are described?
            - **Action Steps**: What actions were taken to address these challenges?
            - **Impact/Outcomes**: What were the results or changes achieved?
         4. Score EACH of the THREE criteria (Impact, Issue, Action) with values between 0.0 and 1.0.
         5. Write detailed justifications for EACH of the three scores based on what you find in the story.
         6. Calculate the composite_score = (impact × 0.4) + (issue × 0.3) + (action × 0.3)
         7. Assign the tier based on the rules above.
         8. Write a brief overall_summary (2-3 sentences).
         9. Return ONLY the JSON object with ALL 10 FIELDS. No extra text, no markdown, no code blocks.

         ## Story Document to Analyze

         {story_content}

         ---

         Analyze the story document above and return the evaluation as a valid JSON object with all 10 required fields.$$),
(3, 1, $$# PII Detection Prompt

         You are a PII Detection Specialist tasked with analyzing educational content for personally identifiable information.

         ## Task
         Analyze the provided text and identify if it contains any personally identifiable information (PII). Return your findings in a structured JSON format.

         ## Context
         You will be analyzing educational barrier and solution content including action steps and impact statements from educational programs and initiatives.

         ## PII Detection Guidelines

         ### Flag as `true` if the text contains:

         - Personal names (students, teachers, parents, community members)
         - Specific addresses, house numbers, or exact locations
         - Phone numbers, email addresses, or identification numbers
         - Specific ages combined with identifying details
         - Any information that could identify an individual

         ### Flag as `false` if the text only contains:

         - General locations (village names, district names without specific addresses)
         - General demographic information (community, caste, gender without names)
         - Age groups or grade levels without identifying details

         ## Instructions
         1. Carefully read and analyze the entire text
         2. Identify any potential PII based on the criteria above
         3. Determine confidence level (0.0 to 1.0 scale)
         4. Provide clear justification for your decision
         5. Return response in the exact JSON format specified

         ## Output Format
         Respond with this exact JSON structure:
         ```json
         {
            "pii_flag": true/false,
            "justification": "Brief explanation of why PII was or was not detected",
            "confidence_score": 0.0
         }
         ```

         ## Requirements
         - Use boolean values (true/false) for pii_flag
         - Keep justification concise and specific
         - Confidence score must be between 0.0 and 1.0
         - Focus only on clear, identifiable PII
         - Do not flag indirect identifiers or unique demographic combinations

         Analyse the following text:
         {text} $$);

-- Populating Themes
INSERT INTO themes (id, name, definition, keywords, examples)
VALUES (
    1,
    'Poverty and Economic Barriers',
    'This theme captures insights where families link irregular school attendance or dropouts to financial hardship. It includes responses describing how poverty forces children to prioritize work over education, how households depend on children’s income for survival, and how limited resources—such as inability to afford uniforms, books, or transport—become barriers to schooling.',
    'Poor, no money, lack of money, financial constraints, child labour, economic constraint, unemployment, financial difficulties',
    $$- Due to economic constraints, families force girls to engage in domestic work or labour, which hinders their education
-  Due to poor financial condition, the girl is not able to study
- Poverty in the society is another challenge that affects education
-Financial constraints and poverty were major challenges for some families in educating their daughters
- Unemployment in the family is a significant problem that affects children's education
-  Financial difficulties greatly affect a child's education $$
),
(
    2,
    'Legal Document linked Barriers',
    'This theme includes responses where children are unable to enroll in school due to missing or incomplete legal documents such as Aadhaar cards, birth certificates, or identity proofs. It captures how lack of proper documentation creates administrative hurdles that keep children out of the education system.',
    'No Aadhar card, no birth certificate, lack of Aadhar card, no ID, no legal document, enrollment barriers due to no Aadhar card',
    $$- Aadhar cards of children have not been made, due to which they are not getting admission in school
- Due to lack of Aadhar cards, schools in the community are facing challenges in enrolling children
-Children are not getting admission due to lack of Aadhaar card, which is affecting their education $$
),
(
    3,
    'Early Marriage',
    'This theme captures responses where child marriage or early marriage prevents girls from continuing their education. It includes situations where early marriage leads to school dropout, limits learning opportunities, or shifts responsibilities toward household duties instead of schooling.',
    'Child marriage, marriage, early marriage',
    $$- Child marriage is a prevalent issue in the community
- Child marriage is prevalent which causes girls to drop out of school before completing their education $$
),
(
    4,
    'Distance and Accessibility Issues',
    'This theme captures challenges that prevent children from attending school due to physical and environmental conditions. It includes long distances to school, poor road or transport infrastructure, and difficulties caused by weather or seasonal factors (such as heavy rains, heat, or floods). These conditions make daily travel to school inconvenient, unreliable, or physically demanding for children.',
    'school is far, distance of school,  no bus, lack of transportation, no road, roads are bad, no school in village, rain, hot, heat, sun , has to walk a lot, long distances, no clean roads',
    $$-  The school is very far from the village
- If the school is far away, children cannot go there
- The child is unable to reach school due to rain
- There is no school in the village, the school is very far from the village, due to this the girls leave their studies midway
- The transportation system is a significant problem, making it difficult for children to reach school on time
- There is a lack of buses for children and teachers to come to class
- Children are not able to go to school because the school is far away and the roads are bad so they cannot go to study
- The child is unable to go to school because of the heat
- Sun, heat, rain and rain create hindrance in studies $$
),
(
    5,
    'Parental Attitudes and Socio-Cultural Barriers',
    'This theme captures responses where parental beliefs, family mindsets, and cultural norms discourage girls from attending school. It includes attitudes such as prioritizing domestic roles for girls, believing education is unnecessary for them, concerns about dowry increasing with higher education, or long-standing traditions that limit girls’ mobility and learning opportunities. These socio-cultural factors collectively shape decisions that keep girls out of school.',
    'Discrimination, cultural beliefs, social pressure, not allowed, marriage, dowry, take care of siblings, girls do not need to study, girls run away, speak foul language',
    $$-  Cultural beliefs that girls do not need education because they will only get married and stay at home are a major challenge
- Gender discrimination is a significant issue in the community, affecting the education and empowerment of girls
- Purdah system is prevalent in Muslim community, due to which we do not send teenage girls out
-  The community believes that educating girls will increase the demand for dowry
-  Social discrimination based on caste or gender also leads to low participation
- Social pressure was identified as another challenge that can prevent girls from pursuing their education
- Girls leave their studies and run away. Due to this fear, parents are unable to provide higher education to their daughters
- Parents are afraid that their daughters might get exposed to foul language and hence do not allow them to go to school
- Girls are not allowed to study for fear of going astray or running away $$
),
(
    6,
    'School Infrastructure and Facility Issues',
    'This theme captures responses highlighting gaps in school facilities and infrastructure gaps.  It includes issues such as inadequate classrooms, lack of basic amenities, insufficient learning resources, as well as delays, inconsistencies, or limited access to government schemes. Together, these systemic gaps reduce the attractiveness and effectiveness of schooling for children and families.',
    'No clean water, toilets, mid day meal, scholarships, playgrounds, classrooms, pads, hygiene, books, uniforms, government schemes, sanitation, infrastructure,  clean, cleanliness, no library, shortage of books, basic facilities',
    $$- Our school children are not given books on time
- The benefits of government schemes are not being received, due to which children are not going to school
- Children did not get uniforms
- Sanitary pads are not provided in school
- Mid day meals are not provided properly to the children in the school
- Lack of toilets in schools
- Girls face significant barriers to accessing education due to poor infrastructure and lack of resources
- Lack of school infrastructure, including poor classrooms, sanitation facilities, water, and transportation, hinders the learning environment
- There is no water in the toilet
- There is no fan facility in my school
- Drinking water is a challenge in schools
- There are not enough playgrounds for sports children
- There are not enough playgrounds for sports children
- The environment around the school is not clean, which affects the health and well-being of children
- The library lacks sufficient books for students $$
),
(
    7,
    'Teacher Capacity and Quality Issues',
    'This theme covers responses highlighting challenges related to insufficient teachers or concerns about teaching quality. It includes issues such as vacant positions, irregular teacher attendance, overburdened staff, and gaps in subject knowledge or pedagogy. These factors affect the learning environment and reduce children’s motivation to attend school regularly.',
    'Shortage, quality issue, teacher not on time,  dont pay attention, slow learning progress etc. , academic progress, lack in academics, Lack of english education',
    $$- There is a shortage of teachers in the school, due to which subject-wise studies are not done
- The work of the teacher is not being done properly in our school
- Attendance of teachers is a problem
- Teachers do not come to school on time
- Teachers do not pay attention to them
- The quality of teachers is a major concern
- The community lacks education, training and management of teachers, which affects the quality of teaching
- Lack of language-specific teachers in school
- Children's learning progress is a challenge
- There is a lack of English medium education in government schools $$
),
(
    8,
    'Safety Concerns',
    'This theme includes responses where children’s school attendance is affected by worries related to their overall safety and security. It covers issues such as unsafe routes, harassment, or any situation that makes families feel that the environment around schooling is not secure enough for children to travel or attend regularly.',
    'Harassment, molestation, eve-teasing, tease, violence, environment around house, house surroundings',
    $$- The presence of stray dogs on the streets as children walk to school raises concerns about their safety
- Violence against women is a significant issue
- It is a challenge for parents in the village to protect their girls from molestation
- Seeing the atmosphere of the village, there is a fear in the minds of parents that someone might tease their daughter
- Musahar children face harassment from other children in school because of their caste, which causes them to develop fear and reluctance to go to school
- The school premises do not have a boundary wall, which poses a security threat
- Girls are harassed on their way to school due to which they are afraid and are refusing to go to school
- The community raised concerns about the environment around the house $$
),
(
    9,
    'Substance Abuse and Addiction',
    'This theme captures responses where children’s education is affected by alcohol or drug use within the family or community, as well as issues like gambling or addiction to online games. These problems create unstable home environments, distract children from studies, and contribute to irregular attendance or dropout.',
    'Addiction, Addicted, Alcohol, Drugs, Alcoholism, online games, mobile phones, Gambling',
    $$- Gambling addiction in children
- Children's education is affected by alcohol
- Drug addiction is a significant challenge that is affecting the education of children in the community
- The father is an alcoholic, so the daughter is being forced to study
- Alcohol addiction in a household member is a problem
- Children use mobile phones more often
- Children are increasingly playing online games using mobile phones $$
),
(
    10,
    'Other Factors',
    'This theme includes responses that do not fit into any of the defined categories but still influence children’s school attendance or learning. It captures unique, context-specific, or less common reasons mentioned by respondents that contribute to educational challenges.',
    'Parent awareness, migration, shy, motivation, does not want to study',
    $$- Parents in the community lack awareness about the importance of education
- Family migration for work causes problems for children
- Parents in villages prefer private schools over government schools
- An 18-year-old girl in the seventh class feels shy and does not attend school due to being older and larger than her classmates, fearing ridicule $$
);


INSERT INTO themes (id, name, definition)
VALUES
(
    11,
    'Unknown/Unclear',
    'Any statements that are not completed, unclear, insufficient can be tagged to this.  This category should not be added in the dashboard'
);