module.exports = {
  apps: [
    {
      name: 'bigdata-simulator',
      script: 'app_optimized.py',
      interpreter: 'python3',
      cwd: '/opt/bigdata-simulator',
      instances: 1,
      autorestart: true,
      max_restarts: 10,
      min_uptime: 30000,
      restart_delay: 5000,
      watch: false,
      env: {
        PYTHONUNBUFFERED: '1'
      },
      error_file: '/var/log/bigdata-simulator-error.log',
      out_file: '/var/log/bigdata-simulator-out.log',
      log_date_format: 'YYYY-MM-DD HH:mm:ss',
      merge_logs: true,
      exp_backoff_restart_delay: 10000
    }
  ]
};
