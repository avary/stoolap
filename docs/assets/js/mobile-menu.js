document.addEventListener('DOMContentLoaded', function() {
  const mobileMenuToggle = document.querySelector('.mobile-menu-toggle');
  const mainNav = document.querySelector('.main-nav');
  
  if (mobileMenuToggle && mainNav) {
    mobileMenuToggle.addEventListener('click', function() {
      this.classList.toggle('active');
      mainNav.classList.toggle('active');
    });
  }
  
  // Close mobile menu when clicking outside
  document.addEventListener('click', function(event) {
    if (
      mainNav && 
      mainNav.classList.contains('active') && 
      !mainNav.contains(event.target) && 
      !mobileMenuToggle.contains(event.target)
    ) {
      mainNav.classList.remove('active');
      mobileMenuToggle.classList.remove('active');
    }
  });
  
  // Close mobile menu when window is resized to desktop size
  window.addEventListener('resize', function() {
    if (mainNav && window.innerWidth > 768) {
      mainNav.classList.remove('active');
      if (mobileMenuToggle) {
        mobileMenuToggle.classList.remove('active');
      }
    }
  });
});