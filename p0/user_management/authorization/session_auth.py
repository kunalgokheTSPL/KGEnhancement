from p0.Login.auth.session_manager import SessionManager
from p0.user_management.authorization.permission_check import normalize_username

session_manager = SessionManager()

def extract_username_from_email_or_username(email: str | None, username: str | None) -> str:
    """Helper to extract username from email prefix or fallback to username."""
    if email and "@" in email:
        result = email.split("@")[0]
    else:
        result = username
    return normalize_username(result)


async def get_username_from_session(session_id: str) -> str | None:
    """Look up the username stored in Redis for a given session_id."""
    if not session_id:
        return None
    try:
        session = await session_manager.get_session(session_id)
        if not session:
            return None
        
        username = extract_username_from_email_or_username(session.get("email"), session.get("username"))
        return username if username else None
    except Exception as e:
        print(f"Error getting session from Redis: {e}")
        return None