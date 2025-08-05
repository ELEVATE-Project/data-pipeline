# Description
These recommendations are intended to promote code quality and team communication during software development. They cover a variety of topics, including ensuring that pull requests are submitted to the correct branch, documenting new methods, preserving consistency across many services, and avoiding typical blunders like accessing APIs or DB queries within loops. Sensitive data should not be uploaded, and changes to environment variables or database models should be executed consistently. Teams may work more effectively and develop higher-quality software by adhering to these standards.


## Type of change
Please choose appropriate options.

- [ ]  Bug fix (non-breaking change which fixes an issue)
- [ ]  New feature (non-breaking change which adds functionality)
- [ ]  Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ]  Enhancement (additive changes to improve performance)
- [ ]  This change requires a documentation update

## Checklist

- [ ]  It's critical to avoid making needless file modifications in contributions, such as adding new lines, console logs, or additional spaces, to guarantee cleaner and more efficient code. Furthermore, eliminating unnecessary imports from a file might enhance code readability and efficiency.
- [ ]  Ensure that the pull request is assigned to the right base branch and that the development branch name contains the JIRA Task Id. Furthermore, each commit message should include the JIRA Task Id in the manner "ED-100: message".
- [ ]  Only update packages if it is mentioned and authorized in the design document, and make sure that you have the required permissions.
- [ ]  Avoid making API and database queries inside a loop as it can lead to performance issues and slow down the system.
- [ ]  When calling another function inside a given function, add comments explaining the purpose and meaning of the passed arguments and expected return values.
- [ ]  If adding a blank argument in a function, add a comment explaining the reason for the blank argument.
- [ ]  Before submitting a pull request, do a self-review of your code to ensure there are no conflicts with the base branch and all comments have been addressed.
- [ ]  Before merging a pull request, it's important to have other team members review it to catch any potential errors or issues
- [ ]  To maintain code integrity, it's important to remove all related changes when removing code during a code review.
- [ ]  If new constants, endpoints, or utility functions are introduced, it is important to check if they already exist in the service to avoid any duplication.
- [ ]  Whenever a new environment variable is added to a service, it's important to ensure that the necessary changes are made to related files such as ".env.sample" and "envVariables.js" to maintain consistency and avoid errors. Additionally, the new environment variable should be added to the devops repository to ensure that it is properly documented and accessible to the team.
- [ ]  When adding a new function to a service, it is important to document it with relevant information such as the name, parameters, and return value in a consistent format across all services. Additionally, if there are any changes to the API response, ensure that the documentation in the controllers is updated accordingly.
- [ ]  Write a clear and concise commit message that describes the changes made.
- [ ]  Maintain consistent function signature and code across all services when adding a function to multiple services. Implement changes to database models in all services that use the same model.
- [ ]  Use only let and const. Do not use var.
- [ ]  Make common functions for repetitive code blocks.
- [ ]  Avoid uploading sensitive information such as secret tokens or passwords in pull requests to ensure data security.
- [ ]  Maintain consistent indentation and spacing throughout the code.
