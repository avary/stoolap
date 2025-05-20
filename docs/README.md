# Stoolap Website

This directory contains the GitHub Pages website for Stoolap, hosted at [stoolap.io](https://stoolap.io).

## Local Development

To run the website locally:

1. Install Jekyll and Bundler:
   ```
   gem install jekyll bundler
   ```

2. Navigate to the `docs` directory:
   ```
   cd docs
   ```

3. Install dependencies:
   ```
   bundle install
   ```

4. Start the local server:
   ```
   bundle exec jekyll serve
   ```

5. Open your browser to `http://localhost:4000`

## Site Structure

- `_config.yml` - Jekyll configuration
- `index.html` - Homepage
- `assets/` - CSS, JavaScript, and images
- `_layouts/` - Page layouts
- `_includes/` - Reusable components
- `_docs/` - Documentation pages

## Deployment

The site is automatically deployed when changes are pushed to the main branch.

## Theme Customization

The website uses a custom theme based on Jekyll's minimal theme with support for:

- Light and dark mode (automatically detects system preference with manual toggle)
- Responsive design for all device sizes
- Encode Sans font
- Stoolap brand colors