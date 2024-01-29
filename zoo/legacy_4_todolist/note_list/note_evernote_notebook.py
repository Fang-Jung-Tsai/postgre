import evernote.edam.userstore.constants as UserStoreConstants
import evernote.edam.type.ttypes as Types
import evernote.edam.error.ttypes as Errors
import evernote.api.client as EvernoteClient

def search_evernote_notebook(api_token, shared_notebook_guid, note_title_query):
    # Connect to Evernote API
    client = EvernoteClient(token=api_token, sandbox=False)
    user_store = client.get_user_store()
    user = user_store.getUser()

    # Create NoteStore client
    note_store = client.get_note_store()

    # Get the shared notebook
    shared_notebook = note_store.getSharedNotebookByAuth(shared_notebook_guid, user.token)

    # Create NoteFilter
    note_filter = Types.NoteFilter()
    note_filter.notebookGuid = shared_notebook.notebookGuid
    note_filter.words = note_title_query

    # Search for notes with NoteFilter
    notes_metadata = note_store.findNotesMetadata(user.token, note_filter, 0, 100, None, False)

    # Get note content for each matching note
    notes_content = []
    for note_metadata in notes_metadata.notes:
        note = note_store.getNote(user.token, note_metadata.guid, True, False, False, False)
        notes_content.append(note.content)

    return notes_content

