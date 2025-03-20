-- 모든 직원의 이름과 연봉 정보만을 조회하는 쿼리를 작성해주세요
SELECT * FROM employees;
-- Frontend 직책을 가진 직원 중에서 연봉이 90000 이하인 직원의 이름과 연봉을 조회하세요.
SELECT `name`, `salary` FROM employees WHERE position = 'Frontend' AND salary <= 90000;
-- PM 직책을 가진 모든 직원의 연봉을 10% 인상한 후 그 결과를 확인하세요.
UPDATE employees SET salary = salary * 1.1 WHERE position = 'PM'; 

SELECT * FROM employees WHERE position = 'PM';
-- 모든 Backend' 직책을 가진 직원의 연봉을 5% 인상하세요.
UPDATE employees SET salary = salary * 1.05 WHERE position = 'Backend';